use crate::object_store::worker::bucket_read;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use datafusion_data_access::Result;
use s3::Bucket;
use std::cmp::min;
use std::io;
use std::io::ErrorKind;

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// The number of bytes read at the end of the parquet file on first read
const DEFAULT_FOOTER_READ_SIZE: usize = 64 * 1024;

// based on https://github.com/apache/arrow-rs/blob/master/parquet/src/file/footer.rs#L46=
pub async fn load_parquet_metadata(
    bucket: &Bucket,
    path: &str,
    file_size: u64,
) -> Result<(u64, Bytes)> {
    // check file is large enough to hold footer
    if file_size < (FOOTER_SIZE as u64) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Invalid Parquet file. Size is smaller than footer, file_size = {}",
                file_size
            ),
        ));
    }

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = min(DEFAULT_FOOTER_READ_SIZE, file_size as usize);
    let default_end_start = file_size - default_end_len as u64;
    let default_len_end_buf = bucket_read(bucket, path, default_end_start, default_end_len).await?;

    let default_len_end = default_len_end_buf.len();
    if default_len_end < FOOTER_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Invalid Parquet file. Size is smaller than footer, len = {}",
                default_len_end
            ),
        ));
    }

    // check this is indeed a parquet file
    if default_len_end_buf[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Invalid Parquet file. Corrupt footer",
        ));
    }

    // get the metadata length from the footer
    let metadata_len =
        LittleEndian::read_i32(&default_len_end_buf[default_end_len - 8..default_end_len - 4])
            as i64;
    if metadata_len < 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Invalid Parquet file. Metadata length is less than zero ({})",
                metadata_len
            ),
        ));
    }
    let footer_metadata_len = FOOTER_SIZE + metadata_len as usize;

    // build up the reader covering the entire metadata
    return if footer_metadata_len > file_size as usize {
        Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Invalid Parquet file. Metadata start is less than zero ({})",
                file_size as i64 - footer_metadata_len as i64
            ),
        ))
    } else if footer_metadata_len < default_len_end {
        // the whole metadata is in the bytes we already read
        Ok((default_end_start, default_len_end_buf))
    } else {
        // the end of file read by default is not long enough, read missing bytes
        let start = file_size - footer_metadata_len as u64;
        let mut complementary_end_read = bucket_read(
            bucket,
            path,
            start,
            FOOTER_SIZE + metadata_len as usize - default_end_len,
        )
        .await?
        .to_vec();
        complementary_end_read.extend(default_len_end_buf);

        Ok((start, Bytes::from(complementary_end_read)))
    };
}
