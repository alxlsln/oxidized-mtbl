use crate::block::{Block, BlockIter};
use crate::compression::decompress;
use crate::error::MtblError;
use crate::reader::ReaderIterType;
use crate::varint::varint_decode64;
use crate::{error::Error, Metadata};
use crate::{metadata, BytesView, FileVersion, METADATA_SIZE};

use async_trait::async_trait;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::Client;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use futures::Stream;
use load_dotenv::load_dotenv;
use std::borrow::Cow;
use std::env;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

load_dotenv!();

pub struct LocalReader {
    file: File,
}

impl LocalReader {
    pub async fn open(path: String) -> Result<Self, Error> {
        let file = File::open(path).await?;

        Ok(Self { file })
    }
}
pub struct S3Reader {
    client: Client,
    key: String,
    bucket: String,
}

impl S3Reader {
    pub async fn new(key: String, bucket: String) -> Result<Self, Error> {
        let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://nas:9000".into());
        let region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let access_key = env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
        let secret_key = env::var("S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                access_key, secret_key, None, None, "dotenv",
            ))
            .region(Region::new(region))
            .endpoint_url(endpoint)
            .force_path_style(true) 
            .build();

        let client = Client::from_conf(s3_config);

        // Check 1 : le service répond et les credentials sont valides.
        client
            .head_bucket()
            .bucket(&bucket)
            .send()
            .await
            .map_err(|e| {
                use aws_smithy_types::error::display::DisplayErrorContext;
                let ctx = DisplayErrorContext(&e);
                eprintln!("S3 check failed: {ctx}");
                Error::from(MtblError::StorageUnavailable)
            })?;

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                eprintln!("S3 object check failed for '{}/{}': {e}", bucket, key);
                Error::from(MtblError::StorageUnavailable)
            })?;

        let content_length = head.content_length().unwrap_or(0);

        Ok(Self {
            client,
            key,
            bucket,
        })
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

#[async_trait]
pub trait AsyncReadAt: Send + Sync {
    async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error>;
}

#[async_trait]
pub trait AsyncFileSize: Send + Sync {
    async fn len(&self) -> Result<u64, Error>;
}

#[async_trait]
impl AsyncReadAt for LocalReader {
    async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; length];
        let mut cloned_file = self.file.try_clone().await?;
        cloned_file.seek(SeekFrom::Start(offset)).await?;
        cloned_file.read_exact(&mut buf).await?;

        Ok(buf)
    }
}

#[async_trait]
impl AsyncFileSize for LocalReader {
    async fn len(&self) -> Result<u64, Error> {
        let file_size = self.file.metadata().await?.len();
        Ok(file_size)
    }
}

#[async_trait]
impl AsyncReadAt for S3Reader {
    async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error> {
        let range = format!("bytes={}-{}", offset, offset + length as u64 - 1);

        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                use aws_smithy_types::error::display::DisplayErrorContext;
                let ctx = DisplayErrorContext(&e);
                eprintln!("S3 check failed: {ctx}");
                Error::from(MtblError::StorageUnavailable)
            })?
            .body
            .collect()
            .await
            .map_err(|e| {
                use aws_smithy_types::error::display::DisplayErrorContext;
                let ctx = DisplayErrorContext(&e);
                eprintln!("S3 collect failed: {ctx}");
                Error::from(MtblError::StorageUnavailable)
            })?;

        Ok(object.to_vec())
    }
}

#[async_trait]
impl AsyncFileSize for S3Reader {
    async fn len(&self) -> Result<u64, Error> {
        dbg!("call len");
        let response = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .send()
            .await
            .map_err(|e| {
                use aws_smithy_types::error::display::DisplayErrorContext;
                let ctx = DisplayErrorContext(&e);
                eprintln!("S3 collect failed: {ctx}");
                Error::from(MtblError::StorageUnavailable)
            })?;

        let file_size = response.content_length().unwrap_or(0);

        Ok(file_size as u64)
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
        // TODO keep data len from read_index_block

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

#[async_trait]
pub trait AsyncReadBytes: Send + Sync {
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, Error>;
}

#[async_trait]
impl AsyncReadBytes for S3Reader {
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, Error> {
        let range = format!("bytes={}-{}", offset, offset + length as u64 - 1);

        let bytes = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                use aws_smithy_types::error::display::DisplayErrorContext;
                let ctx = DisplayErrorContext(&e);
                eprintln!("S3 check failed: {ctx}");
                Error::from(MtblError::StorageUnavailable)
            })?
            .body
            .collect()
            .await
            .map_err(|e| {
                use aws_smithy_types::error::display::DisplayErrorContext;
                let ctx = DisplayErrorContext(&e);
                eprintln!("S3 collect failed: {ctx}");
                Error::from(MtblError::StorageUnavailable)
            })?;
        Ok(bytes.into_bytes())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockMeta {
    offset: u64,
    len: usize,
}

#[derive(Debug, Clone)]
pub struct GetRange {
    pub offset: u64,
    pub len: usize,
    pub blocks: Vec<BlockMeta>,
}

pub fn coalesce(blocks: &[BlockMeta], max_range: usize) -> Vec<GetRange> {
    let mut result: Vec<GetRange> = Vec::new();

    for block in blocks {
        if let Some(last) = result.last_mut() {
            let end = last.offset + last.len as u64;

            if block.offset == end && block.len <= max_range.saturating_sub(last.len) {
                last.len += block.len;
                last.blocks.push(*block);
                continue;
            }
        }

        result.push(GetRange {
            offset: block.offset,
            len: block.len,
            blocks: vec![*block],
        });
    }

    result
}

pub struct WindowedReaderIter<R> {
    // index_iter
    blocks: Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>,
    block_iter: Option<BlockIter<Vec<u8>>>,
    first: bool,
    valid: bool,
    k: Vec<u8>,
    it_type: ReaderIterType,
    _r: PhantomData<R>,
}

pub type BlockStream = Pin<Box<dyn futures::Stream<Item = Result<Bytes, Error>> + Send>>;
type BlockStreamItem = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub fn block_stream(
    source: Arc<dyn AsyncReadBytes>, // need to live after block_stream
    ranges: Vec<GetRange>,
    k: usize,
) -> BlockStreamItem {
    stream::iter(ranges)
        .map(move |range| {
            let source = Arc::clone(&source);
            async move {
                let data = source.read_range(range.offset, range.len).await?;
                Ok((range, data))
            }
        })
        .buffered(k)
        .flat_map(|fetched| -> BlockStreamItem {
            match fetched {
                Ok((range, data)) => {
                    let base = range.offset;
                    stream::iter(range.blocks)
                        .map(move |block| {
                            let start = (block.offset - base) as usize;
                            Ok(data.slice(start..start + block.len))
                        })
                        .boxed() // <- type erasure
                }
                Err(e) => stream::iter(vec![Err(e)]).boxed(), // <- même type effacé
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use std::time::Duration;

    use indicatif::MultiProgress;
    use indicatif::{ProgressBar, ProgressStyle};

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

    #[tokio::test]
    async fn test_async_read_bytes() -> Result<(), Error> {
        let s3_reader = S3Reader::new("test-data.mtbl".to_owned(), "mtbl-test".to_owned()).await?;

        const OFFSET: u64 = 4096;
        const LEN: usize = 100;

        let old: Vec<u8> = s3_reader.read_at(OFFSET, LEN).await.unwrap();
        let new: bytes::Bytes = s3_reader.read_range(OFFSET, LEN).await.unwrap();

        assert_eq!(old.len(), LEN);
        assert_eq!(new.len(), LEN);
        assert_eq!(new.as_ref(), old.as_slice());

        Ok(())
    }

    async fn iter_s3(mpb: MultiProgress) -> Result<(), Error> {
        let s3_reader = S3Reader::new("test-data.mtbl".to_owned(), "mtbl-test".to_owned()).await?;
        let reader = AsyncReader::new(s3_reader).await?;
        let mut iter = reader.into_iter().await?;

        let pb = mpb.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [S3]    {pos} items ({per_sec}) elapsed: {elapsed}")
                .unwrap(),
        );
        pb.enable_steady_tick(Duration::from_millis(250));

        while let Some((_key, _value)) = iter.next().await? {
            pb.inc(1);
        }

        pb.finish_with_message(format!("[S3]    terminé : {} items", pb.position()));
        Ok(())
    }

    async fn iter_local(path: &str, mpb: MultiProgress) -> Result<(), Error> {
        let local_reader = LocalReader::open(path.to_owned()).await?;
        let reader = AsyncReader::new(local_reader).await?;
        let mut iter = reader.into_iter().await?;

        let pb = mpb.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.blue} [LOCAL] {pos} items ({per_sec}) elapsed: {elapsed}")
                .unwrap(),
        );
        pb.enable_steady_tick(Duration::from_millis(250));

        while let Some((_key, _value)) = iter.next().await? {
            pb.inc(1);
        }

        pb.finish_with_message(format!("[LOCAL] terminé : {} items", pb.position()));
        Ok(())
    }

    // #[tokio::test]
    // async fn test_coalesce() -> Result<(), Error> {
    //     let blocks = vec![
    //         BlockMeta { offset: 0, len: 10 },
    //         BlockMeta { offset: 10, len: 5 },
    //         BlockMeta {
    //             offset: 20,
    //             len: 10,
    //         },
    //         BlockMeta {
    //             offset: 100,
    //             len: 10,
    //         },
    //     ];

    //     let merged = coalesce(&blocks, 32);

    //     let result = vec![
    //         BlockMeta { offset: 0, len: 15 },
    //         BlockMeta {
    //             offset: 20,
    //             len: 10,
    //         },
    //         BlockMeta {
    //             offset: 100,
    //             len: 10,
    //         },
    //     ];

    //     assert_eq!(merged, result);
    //     Ok(())
    // }

    struct FakeSource {
        data: Bytes,
    }

    #[async_trait]
    impl AsyncReadBytes for FakeSource {
        async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, Error> {
            let s = offset as usize;
            Ok(self.data.slice(s..s + length))
        }
    }

    fn m(offset: u64, len: usize) -> BlockMeta {
        BlockMeta { offset, len }
    }

    #[tokio::test]
    async fn tes_block_stream_return_order() {
        let data = Bytes::from((0u32..32).map(|i| i as u8).collect::<Vec<u8>>());
        let metas = vec![m(0, 8), m(8, 8), m(16, 8), m(24, 8)];
        let ranges = coalesce(&metas, 16); 

        let source = Arc::new(FakeSource { data: data.clone() });
        let stream = block_stream(source, ranges, 2);

        let got: Vec<Bytes> = stream.map(|b| b.expect("")).collect().await;

        assert_eq!(got.len(), 4);
        assert_eq!(got[0], data.slice(0..8));
        assert_eq!(got[1], data.slice(8..16));
        assert_eq!(got[3], data.slice(24..32));
    }

    #[tokio::test]
    async fn test_iter_both() -> Result<(), Error> {
        let path = "test-data.mtbl".to_owned();
        let mpb = MultiProgress::new();

        let mpb_local = mpb.clone();

        let s3_task = tokio::spawn(async move {
            if let Err(e) = iter_s3(mpb).await {
                eprintln!("[S3] erreur : {e}");
            }
        });

        let local_task = tokio::spawn(async move {
            if let Err(e) = iter_local(&path.clone(), mpb_local).await {
                eprintln!("[LOCAL] erreur : {e}");
            }
        });

        let _ = tokio::join!(s3_task, local_task);
        Ok(())
    }
}
