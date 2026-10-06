use crate::block::{Block, BlockIter};
use crate::compression::decompress;
use crate::error::MtblError;
use crate::reader::ReaderIterType;
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

    pub async fn get(self, key: &[u8]) -> Result<Option<AsyncReaderGet>, Error> {
        let mut iter = AsyncReaderIter::new_get(self, key).await?;
        match iter.next().await? {
            // Some((_, value)) => Ok(Some(value.to_vec())),
            Some(_) => match iter.block_iter {
                Some(block_iter) => Ok(AsyncReaderGet::new(block_iter)),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    pub async fn into_iter(self) -> Result<AsyncReaderIter<R>, Error> {
        AsyncReaderIter::new(self).await
    }

    pub async fn iter_from(self, start: &[u8]) -> Result<AsyncReaderIter<R>, Error> {
        AsyncReaderIter::new_from(self, start).await
    }

    pub async fn iter_prefix(self, prefix: &[u8]) -> Result<AsyncReaderIter<R>, Error> {
        AsyncReaderIter::new_get_prefix(self, prefix).await
    }

    pub async fn iter_range(self, start: &[u8], end: &[u8]) -> Result<AsyncReaderIter<R>, Error> {
        AsyncReaderIter::new_get_range(self, start, end).await
    }
}

pub struct AsyncReaderGet {
    block: Arc<Block<Vec<u8>>>,
    val_offset: usize,
    val_len: usize,
}

impl AsyncReaderGet {
    fn new(block_iter: BlockIter<Vec<u8>>) -> Option<Self> {
        let (offset, length) = block_iter.val?;

        Some(Self {
            block: block_iter.block,
            val_offset: offset,
            val_len: length,
        })
    }
}
impl AsRef<[u8]> for AsyncReaderGet {
    fn as_ref(&self) -> &[u8] {
        &self.block.as_ref().as_ref()[self.val_offset..self.val_offset + self.val_len]
    }
}

pub struct AsyncReaderIter<R>
where
    R: AsyncReadAt + AsyncFileSize,
{
    reader: AsyncReader<R>,
    block_iter: Option<BlockIter<Vec<u8>>>,
    index_iter: BlockIter<Vec<u8>>,
    k: Vec<u8>,
    first: bool,
    valid: bool,
    it_type: ReaderIterType,
}

impl<R> AsyncReaderIter<R>
where
    R: AsyncReadAt + AsyncFileSize,
{
    async fn new(reader: AsyncReader<R>) -> Result<Self, Error> {
        let mut index_iter = BlockIter::init(reader.index.clone());
        index_iter.seek_to_first();

        let block_iter = match reader.block_at_index(&index_iter).await? {
            Some(b) => {
                let mut block_iter = BlockIter::init(Arc::new(b));
                block_iter.seek_to_first();
                Some(block_iter)
            }
            None => None,
        };

        let valid = block_iter.is_some();

        Ok(AsyncReaderIter {
            reader: reader,
            index_iter,
            block_iter,
            k: Vec::new(),
            first: true,
            valid: valid,
            it_type: ReaderIterType::Iter,
        })
    }

    async fn new_from(reader: AsyncReader<R>, k: &[u8]) -> Result<Self, Error> {
        let mut index_iter = BlockIter::init(reader.index.clone());
        index_iter.seek(k);

        let block_iter = match reader.block_at_index(&index_iter).await? {
            Some(b) => {
                let mut block_iter = BlockIter::init(Arc::new(b));
                block_iter.seek(k);
                Some(block_iter)
            }
            None => None,
        };

        let valid = block_iter.is_some();

        Ok(AsyncReaderIter {
            reader: reader,
            index_iter,
            block_iter,
            k: Vec::new(),
            first: true,
            valid: valid,
            it_type: ReaderIterType::Iter,
        })
    }

    async fn new_get(r: AsyncReader<R>, key: &[u8]) -> Result<Self, Error> {
        let mut iter = AsyncReaderIter::new_from(r, key).await?;
        iter.k.extend_from_slice(key);
        iter.it_type = ReaderIterType::Get;
        Ok(iter)
    }

    async fn new_get_prefix(r: AsyncReader<R>, prefix: &[u8]) -> Result<Self, Error> {
        let mut iter = Self::new_from(r, prefix).await?;
        iter.k.extend_from_slice(prefix);
        iter.it_type = ReaderIterType::GetPrefix;
        Ok(iter)
    }

    async fn new_get_range(r: AsyncReader<R>, start: &[u8], end: &[u8]) -> Result<Self, Error> {
        let mut iter = Self::new_from(r, start).await?;
        iter.k.extend_from_slice(end);
        iter.it_type = ReaderIterType::GetRange;
        Ok(iter)
    }

    async fn load_current_block(&mut self) -> Result<bool, Error> {
        let block = self.reader.block_at_index(&self.index_iter).await?;

        match block {
            Some(block) => {
                let mut block_iter = BlockIter::init(Arc::new(block));
                block_iter.seek_to_first();

                self.block_iter = Some(block_iter);

                Ok(true)
            }
            None => {
                self.block_iter = None;
                Ok(false)
            }
        }
    }

    pub async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        if !self.valid {
            return Ok(None);
        }
        let block_iter = match self.block_iter.as_mut() {
            Some(block_iter) => block_iter,
            None => return Err(Error::from(MtblError::InvalidBlock)),
        };

        if !self.first {
            block_iter.next();
        }
        self.first = false;

        let (key, val) = match block_iter.get() {
            Some((key, value)) => (key.to_vec(), value.to_vec()),
            None => {
                self.valid = false;

                // Init next data into block_iter and get first value
                if !self.index_iter.next() {
                    return Ok(None);
                }

                if !self.load_current_block().await? {
                    return Ok(None);
                };

                let block_iter = match self.block_iter.as_mut() {
                    Some(block_iter) => block_iter,
                    None => {
                        return Err(Error::from(MtblError::InvalidBlock));
                    }
                };

                match block_iter.get() {
                    Some((key, value)) => {
                        self.valid = true;
                        (key.to_vec(), value.to_vec())
                    }
                    None => {
                        return Err(Error::from(MtblError::InvalidBlock));
                    }
                }
            }
        };

        match self.it_type {
            ReaderIterType::Iter => (),
            ReaderIterType::Get => {
                if key != self.k.as_slice() {
                    self.valid = false;
                }
            }
            ReaderIterType::GetPrefix => {
                if !(self.k.len() <= key.len() && key.starts_with(&self.k)) {
                    self.valid = false;
                }
            }
            ReaderIterType::GetRange => {
                if key > self.k {
                    self.valid = false;
                }
            }
        }

        if self.valid {
            Ok(Some((key, val)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::OnceLock;

    use crate::compression::CompressionType;
    use crate::writer::WriterBuilder;

    use crate::block::BlockIter;
    use crate::error::{Error, MtblError};
    use crate::varint::varint_decode64;

    use super::*;

    const TEST_ENTRIES: usize = 300_000;

    fn test_file() -> PathBuf {
        static TEST_FILE: OnceLock<PathBuf> = OnceLock::new();

        TEST_FILE
            .get_or_init(|| {
                let path = env::temp_dir().join(format!(
                    "oxidized-mtbl-async-reader-test-{}.data",
                    std::process::id()
                ));

                // Generate the test database only once.
                if !path.exists() {
                    let file = File::create(&path).expect("failed to create test file");

                    let mut writer = WriterBuilder::new()
                        .compression_type(CompressionType::Snappy)
                        .build(file);

                    for i in 0..TEST_ENTRIES {
                        let key = format!("{:010}", i);
                        let value = format!("{:010}", i).repeat(i / 10_000);

                        writer
                            .insert(key, value)
                            .expect("failed to insert test entry");
                    }

                    writer.finish().expect("failed to finish test writer");
                }

                path
            })
            .clone()
    }

    async fn open_test_reader() -> Result<AsyncReader<LocalReader>, Error> {
        let path = test_file();

        let local_reader = LocalReader::open(path.to_string_lossy().into_owned()).await?;

        AsyncReader::new(local_reader).await
    }

    fn expected_value(i: usize) -> Vec<u8> {
        format!("{:010}", i).repeat(i / 10_000).into_bytes()
    }

    #[tokio::test]
    async fn test_open() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        assert_eq!(reader.metadata.count_entries as usize, TEST_ENTRIES);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_index_block() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let index = reader.index.clone();

        let mut index_iter = BlockIter::init(index);
        index_iter.seek_to_first();

        let (key, value) = index_iter.get().ok_or(MtblError::InvalidBlock)?;

        println!("first index key   = {:?}", key);
        println!("first index value = {:?}", value);

        let mut offset = 0;
        let offset_len = varint_decode64(value, &mut offset);

        println!("offset_len = {offset_len}");
        println!("offset     = {offset}");

        let block = reader.block_at_offset(offset as usize).await?;

        let mut block_iter = BlockIter::init(Arc::new(block));
        block_iter.seek_to_first();

        let (key, value) = block_iter.get().ok_or(MtblError::InvalidBlock)?;

        println!("first block key   = {}", String::from_utf8_lossy(key));
        println!("first block value = {}", String::from_utf8_lossy(value));

        assert_eq!(key, b"0000000000");
        assert_eq!(value, b"");

        Ok(())
    }

    #[tokio::test]
    async fn test_find_block() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let block = reader
            .find_block(b"0000012345")
            .await?
            .ok_or(MtblError::InvalidBlock)?;

        let mut block_iter = BlockIter::init(Arc::new(block));
        block_iter.seek(b"0000012345");

        let (key, value) = block_iter.get().ok_or(MtblError::InvalidBlock)?;

        assert_eq!(key, b"0000012345");
        assert_eq!(value, expected_value(12_345));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_key() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let value = reader.get(b"0000012345").await?.expect("key should exist");

        assert_eq!(value.as_ref(), expected_value(12_345).as_slice());

        Ok(())
    }

    #[tokio::test]
    async fn test_iter() -> Result<(), Error> {
        let reader = open_test_reader().await?;
        let mut iter = reader.into_iter().await?;

        let mut count = 0;

        while let Some((key, value)) = iter.next().await? {
            let expected_key = format!("{:010}", count);

            assert_eq!(key, expected_key.as_bytes(), "invalid key at index {count}");

            assert_eq!(
                value,
                expected_value(count),
                "invalid value for key {expected_key}"
            );

            count += 1;
        }

        assert_eq!(count, TEST_ENTRIES);

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_from() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let start = 123_456usize;

        let mut iter = reader
            .iter_from(format!("{:010}", start).as_bytes())
            .await?;

        let mut count = 0;

        while let Some((key, value)) = iter.next().await? {
            let i = start + count;

            assert_eq!(key, format!("{:010}", i).as_bytes());
            assert_eq!(value, expected_value(i));

            count += 1;
        }

        assert_eq!(count, TEST_ENTRIES - start);

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_from_middle_of_key_space() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let start = 250_000usize;

        let mut iter = reader
            .iter_from(format!("{:010}", start).as_bytes())
            .await?;

        let first = iter
            .next()
            .await?
            .expect("iterator should contain the start key");

        assert_eq!(first.0, b"0000250000");
        assert_eq!(first.1, expected_value(start));

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_prefix() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        // Keys are 10 decimal digits.
        //
        // "00000" matches:
        //
        // 0000000000 ... 0000099999
        //
        // => 100_000 entries.
        let prefix = b"00000";

        let mut iter = reader.iter_prefix(prefix).await?;

        let mut count = 0;

        while let Some((key, value)) = iter.next().await? {
            assert!(
                key.starts_with(prefix),
                "key {:?} does not match prefix {:?}",
                key,
                prefix
            );

            let i = count;

            assert_eq!(key, format!("{:010}", i).as_bytes());
            assert_eq!(value, expected_value(i));

            count += 1;
        }

        assert_eq!(count, 100_000);

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_prefix_small_range() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        // Matches exactly:
        //
        // 0000123000 ... 0000123999
        //
        // => 10_000 entries.
        let prefix = b"0000123";

        let mut iter = reader.iter_prefix(prefix).await?;

        let mut count = 0;

        while let Some((key, value)) = iter.next().await? {
            let i = 123_000 + count;
            assert!(key.starts_with(prefix));
            assert_eq!(key, format!("{:010}", i).as_bytes());
            assert_eq!(value, expected_value(i));

            count += 1;
        }

        assert_eq!(count, 1_000);

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_range() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let start = 123_450usize;
        let end = 123_460usize;

        let mut iter = reader
            .iter_range(
                format!("{:010}", start).as_bytes(),
                format!("{:010}", end).as_bytes(),
            )
            .await?;

        let mut keys = Vec::new();

        while let Some((key, value)) = iter.next().await? {
            let key_string = String::from_utf8(key.clone()).unwrap();

            keys.push(key.clone());

            let i: usize = key_string.parse().unwrap();

            assert!((start..=end).contains(&i));
            assert_eq!(value, expected_value(i));
        }

        assert_eq!(keys.len(), end - start + 1);

        assert_eq!(keys.first().unwrap(), b"0000123450");
        assert_eq!(keys.last().unwrap(), b"0000123460");

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_range_across_blocks() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        // Deliberately use a large range so that the iterator has to
        // transition between multiple data blocks.
        let start = 50_000usize;
        let end = 100_000usize;

        let mut iter = reader
            .iter_range(
                format!("{:010}", start).as_bytes(),
                format!("{:010}", end).as_bytes(),
            )
            .await?;

        let mut count = 0;

        while let Some((key, value)) = iter.next().await? {
            let i = start + count;

            assert_eq!(key, format!("{:010}", i).as_bytes());
            assert_eq!(value, expected_value(i));

            count += 1;
        }

        assert_eq!(count, end - start + 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_range_empty() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let mut iter = reader.iter_range(b"0001000000", b"0000999999").await?;

        assert!(iter.next().await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_iter_from_end() -> Result<(), Error> {
        let reader = open_test_reader().await?;

        let start = TEST_ENTRIES - 1;

        let mut iter = reader
            .iter_from(format!("{:010}", start).as_bytes())
            .await?;

        let first = iter.next().await?.expect("last entry should exist");

        assert_eq!(first.0, format!("{:010}", start).as_bytes());
        assert_eq!(first.1, expected_value(start));

        assert!(iter.next().await?.is_none());

        Ok(())
    }
}
