use crate::block::{Block, BlockIter};
use crate::compression::decompress;
use crate::error::MtblError;
use crate::varint::varint_decode64;
use crate::{error::Error, Metadata};
use crate::{metadata, BytesView, FileVersion, METADATA_SIZE};

use byteorder::{ByteOrder, LittleEndian};
use std::borrow::Cow;
use std::mem;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

pub struct LocalReader {
    file: File,
}

impl LocalReader {
    pub async fn open(path: String) -> Result<Self, Error> {
        let file = File::open(path).await?;

        Ok(Self { file })
    }
}

pub struct AsyncReader<R>
where
    R: AsyncReadAt + AsyncFileSize,
{
    reader: R,
    metadata: Metadata,
    index: Arc<Block<Vec<u8>>>,
}

pub trait AsyncReadAt {
    async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error>;
}

impl AsyncReadAt for LocalReader {
    async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; length];
        let mut cloned_file = self.file.try_clone().await?;
        cloned_file.seek(SeekFrom::Start(offset)).await?;
        cloned_file.read_exact(&mut buf).await?;

        Ok(buf)
    }
}

pub trait AsyncFileSize {
    async fn len(&self) -> Result<u64, Error>;
}

impl AsyncFileSize for LocalReader {
    async fn len(&self) -> Result<u64, Error> {
        let file_size = self.file.metadata().await?.len();
        Ok(file_size)
    }
}

impl<R> AsyncReadAt for AsyncReader<R>
where
    R: AsyncReadAt + AsyncFileSize,
{
    async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error> {
        self.reader.read_at(offset, length).await
    }
}

impl<R> AsyncFileSize for AsyncReader<R>
where
    R: AsyncReadAt + AsyncFileSize,
{
    async fn len(&self) -> Result<u64, Error> {
        self.reader.len().await
    }
}

impl<R> AsyncReader<R>
where
    R: AsyncReadAt + AsyncFileSize,
{
    pub async fn new(reader: R) -> Result<Self, Error> {
        let file_size = reader.len().await?;
        let metadata = METADATA_SIZE as u64;
        if file_size < metadata {
            return Err(Error::from(MtblError::InvalidMetadataSize));
        }
        let metadata_offset = file_size - metadata;
        let metadata_buf = reader.read_at(metadata_offset, METADATA_SIZE).await?;
        let metadata = Metadata::read_from_bytes(&metadata_buf)?;

        let index = Arc::new(Self::read_index_block(&reader, &metadata).await?);

        Ok(Self {
            reader,
            metadata,
            index,
        })
    }

    async fn read_index_block(reader: &R, metadata: &Metadata) -> Result<Block<Vec<u8>>, Error> {
        // Sanitize the index block offset.
        // We calculate the maximum possible index block offset for this file to
        // be the total size of the file (r->len_data) minus the length of the
        // metadata block (METADATA_SIZE) minus the length of the minimum
        // sized block, which requires 4 fixed-length 32-bit integers (16 bytes).
        // FIXME why do I get 13 bytes!
        let max_index_block_offset = reader.len().await? - METADATA_SIZE as u64 - 13;
        if metadata.index_block_offset > max_index_block_offset {
            return Err(Error::from(MtblError::InvalidIndexBlockOffset));
        }

        let index_len_len: usize;
        let index_len: usize;
        if metadata.file_version == FileVersion::FormatV1 {
            index_len_len = mem::size_of::<u32>();
            index_len =
                LittleEndian::read_u32(&reader.read_at(metadata.index_block_offset, 4).await?)
                    as usize;
        } else {
            let mut tmp = 0;
            index_len_len = varint_decode64(
                &reader.read_at(metadata.index_block_offset, 10).await?,
                &mut tmp,
            );
            index_len = tmp as usize;
            if index_len as u64 != tmp {
                return Err(Error::from(MtblError::InvalidIndexLength));
            }
        }
        let start = metadata.index_block_offset as usize + index_len_len + mem::size_of::<u32>();
        let index_data = BytesView::from(reader.read_at(start as u64, index_len).await?);

        let index = Block::init(index_data).ok_or(MtblError::InvalidBlock)?;

        Ok(index)
    }

    async fn block_at_offset(&self, offset: usize) -> Result<Block<Vec<u8>>, Error> {
        // assert!(offset < self.data.len());
        // keep data len from read_index_block

        let raw_contents_size_len: usize;
        let raw_contents_size: usize;

        if self.metadata.file_version == FileVersion::FormatV1 {
            raw_contents_size_len = mem::size_of::<u32>();
            raw_contents_size =
                LittleEndian::read_u32(&self.reader.read_at(offset as u64, 4).await?) as usize;
        } else {
            let mut tmp = 0;
            raw_contents_size_len =
                varint_decode64(&self.reader.read_at(offset as u64, 10).await?, &mut tmp);
            raw_contents_size = tmp as usize;
            assert_eq!(raw_contents_size as u64, tmp);
        }

        let raw_start = offset + raw_contents_size_len + mem::size_of::<u32>();
        let raw_contents: Vec<u8> = self
            .reader
            .read_at(raw_start as u64, raw_contents_size)
            .await?;

        let data: Cow<'_, [u8]> = decompress(self.metadata.compression_algorithm, &raw_contents)?;
        let data: BytesView<Vec<u8>> = match data {
            Cow::Borrowed(_) => BytesView::from_bytes(raw_contents),
            Cow::Owned(bytes) => BytesView::from_bytes(bytes),
        };

        let block = Block::init(data).ok_or(MtblError::InvalidBlock)?;

        Ok(block)
    }

    async fn block_at_index(
        &self,
        index_iter: &BlockIter<Vec<u8>>,
    ) -> Result<Option<Block<Vec<u8>>>, Error> {
        match index_iter.get() {
            Some((_key, value)) => {
                let mut offset = 0;
                varint_decode64(value, &mut offset);

                self.block_at_offset(offset as usize).await.map(Some)
            }
            None => Ok(None),
        }
    }

    async fn find_block(&self, key: &[u8]) -> Result<Option<Block<Vec<u8>>>, Error> {
        let mut index_iter = BlockIter::init(self.index.clone());
        index_iter.seek(key);
        self.block_at_index(&index_iter).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let block = match self.find_block(key).await? {
            Some(block) => block,
            None => return Ok(None),
        };

        let mut block_iter = BlockIter::init(Arc::new(block));
        block_iter.seek(key);

        let (find_key, value) = match block_iter.get() {
            Some(entry) => entry,
            None => return Ok(None),
        };

        if key == find_key {
            Ok(Some(value.to_vec()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockIter;

    use super::*;

    #[tokio::test]
    async fn test_open() -> Result<(), Error> {
        let test_path = "./test-verify-good1.data";
        let local_reader = LocalReader::open(test_path.to_string()).await?;
        let reader = AsyncReader::new(local_reader).await?;
        assert_eq!(reader.metadata.count_entries, 128);
        let buf = reader.read_at(10, 10).await?;
        assert_eq!(
            buf,
            vec![6u8, 107u8, 101u8, 121u8, 48u8, 48u8, 48u8, 118u8, 97u8, 108u8]
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_read_index_block() -> Result<(), Error> {
        let test_path = "./test-verify-good1.data";

        let local_reader = LocalReader::open(test_path.to_string()).await?;
        let reader = AsyncReader::new(local_reader).await?;

        let index = reader.index.clone();

        let mut index_iter = BlockIter::init(index);
        index_iter.seek_to_first();

        let (key, value) = index_iter.get().ok_or(MtblError::InvalidBlock)?;

        println!("key   = {:?}", key);
        println!("value = {:?}", value);

        let mut offset = 0;
        let offset_len = varint_decode64(value, &mut offset);

        println!("offset_len = {offset_len}");
        println!("offset = {offset}");

        let block = reader.block_at_offset(offset as usize).await?;
        let mut block_iter = BlockIter::init(Arc::new(block));
        block_iter.seek_to_first();

        let (key, value) = block_iter.get().ok_or(MtblError::InvalidBlock)?;

        println!("key   = {:?}", key);
        println!("value = {:?}", value);

        Ok(())
    }

    #[tokio::test]
    async fn test_find_block() -> Result<(), Error> {
        let test_path = "./test-verify-good1.data";

        let local_reader = LocalReader::open(test_path.to_string()).await?;
        let reader = AsyncReader::new(local_reader).await?;

        let block = reader.find_block(b"key095").await?.unwrap();
        let mut block_iter = BlockIter::init(Arc::new(block));
        block_iter.seek(b"key095");

        let (key, value) = block_iter.get().ok_or(MtblError::InvalidBlock)?;

        println!("key   = {:}", String::from_utf8_lossy(&key));
        println!("value = {:?}", String::from_utf8_lossy(&value));

        assert_eq!(value, b"val095");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_key() -> Result<(), Error> {
        let test_path = "./test-verify-good1.data";

        let local_reader = LocalReader::open(test_path.to_string()).await?;
        let reader = AsyncReader::new(local_reader).await?;

        let value = reader.get(b"key094").await?;
        assert_eq!(value, Some(b"val094".to_vec()));
        let value = reader.get(b"key0955").await?;
        assert_eq!(value, None);
        let value = reader.get(b"aaaa5").await?;
        assert_eq!(value, None);

        Ok(())
    }
}
