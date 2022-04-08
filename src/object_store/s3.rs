// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ObjectStore implementation for the Amazon S3 API

use std::io;
use std::io::{Cursor, ErrorKind, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use async_trait::async_trait;
use futures::{stream, AsyncRead};

use datafusion_data_access::{FileMeta, Result, SizedFile};

use crate::object_store::stream::{RangedStreamer, SeekOutput};
use crate::object_store::worker::{BucketWorker, GetObjectRange};
use datafusion_data_access::object_store::{
    FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use futures::future::BoxFuture;
use s3::command::Command;
use s3::creds::Credentials;
use s3::{Bucket, Region};
use tokio::runtime::Runtime;

/// `ObjectStore` implementation for the Amazon S3 API
#[derive(Debug)]
pub struct S3FileSystem {
    bucket: Arc<Bucket>,
    worker_tx: Arc<mpsc::Sender<GetObjectRange>>,
    counter: AtomicUsize,
}

impl S3FileSystem {
    /// Create new `ObjectStore`
    pub fn new_custom(
        bucket_name: &str,
        endpoint: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
    ) -> Result<Self> {
        // TODO: retry configuration

        let region = Region::Custom {
            region: "".to_string(),
            endpoint: endpoint.to_string(),
        };
        let credentials = Credentials::new(access_key, secret_key, None, None, None)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let mut bucket = Bucket::new(bucket_name, region, credentials).unwrap();
        bucket.set_path_style();
        let bucket = Arc::new(bucket);

        let (tx, rx) = mpsc::channel(100);
        let worker = BucketWorker::new(bucket.clone(), rx);

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let mut worker = worker;
                worker.wait_for_io().await
            });
        });

        Ok(Self {
            bucket,
            worker_tx: Arc::new(tx),
            counter: Default::default(),
        })
    }
}

#[async_trait]
impl ObjectStore for S3FileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let prefix = if let Some((_scheme, path)) = prefix.split_once("://") {
            path
        } else {
            prefix
        };

        let objects = self
            .bucket
            .list(prefix.to_string(), None)
            .await
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?
            .into_iter()
            .flat_map(|list| list.contents);

        /*
                for o in objects.clone() {
                    println!("loading metadata for: {o:?}");
                    let length = o.size as usize;

                    let bucket = self.bucket.clone();
                    let path = Arc::new(o.key.clone());
                    let range_get = Arc::new(move |start: u64, length: usize| {
                        let bucket = bucket.clone();
                        let path = path.clone();
                        Box::pin(async move {
                            let path = path.clone();
                            println!(
                                "requested: {} kb, start={start}, length={length}",
                                length / 1024
                            );
                            let (mut data, _) = bucket
                                .get_object_range(path.as_str(), start, Some(start + length as u64))
                                .await
                                .map_err(|x| {
                                    std::io::Error::new(std::io::ErrorKind::Other, x.to_string())
                                })?;

                            println!("received: {} b", data.len());
                            data.truncate(length);
                            Ok(SeekOutput { start, data })
                        }) as BoxFuture<'static, std::io::Result<SeekOutput>>
                    });

                    let mut reader = RangedStreamer::new(length, 1024 * 1024, range_get);

                    let metadata = parquet2::read::read_metadata_async(&mut reader)
                        .await
                        .unwrap();

                    // metadata
                    println!(
                        "number of rows: {} row_groups: {}",
                        metadata.num_rows,
                        metadata.row_groups.len()
                    );
                }
        */

        let result = stream::iter(objects.map(move |object| {
            Ok(FileMeta {
                sized_file: SizedFile {
                    path: object.key,
                    size: object.size,
                },
                last_modified: Some(
                    object.last_modified.parse().expect("invalid datetime"), // .map_err(|e| io::Error::new(ErrorKind::Other, e))?,
                ),
            })
        }));

        Ok(Box::pin(result))
    }

    async fn list_dir(&self, _prefix: &str, _delimiter: Option<String>) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        let id = self.counter.fetch_add(1, Ordering::Acquire);
        Ok(Arc::new(S3CompatibleFileReader {
            id,
            bucket: self.bucket.clone(),
            file,
            worker_tx: self.worker_tx.clone(),
        }))
    }
}

struct S3CompatibleFileReader {
    id: usize,
    bucket: Arc<Bucket>,
    file: SizedFile,
    worker_tx: Arc<mpsc::Sender<GetObjectRange>>,
}

#[async_trait]
impl ObjectReader for S3CompatibleFileReader {
    async fn chunk_reader(&self, _start: u64, _length: usize) -> Result<Box<dyn AsyncRead>> {
        todo!("implement once async file readers are available (arrow-rs#78, arrow-rs#111)")
    }

    fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>> {
        // TODO: get from cache

        let (tx, rx) = std::sync::mpsc::channel();
        let req = GetObjectRange {
            id: self.id,
            path: self.file.path.clone(),
            start,
            length,
            tx,
        };

        self.worker_tx.try_send(req).unwrap();

        let (bytes, _status_code) = rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|e| io::Error::new(ErrorKind::Other, e))??;
        // println!("file: {} get {} bytes", self.file.path, bytes.len());

        // save to cache

        Ok(Box::new(Cursor::new(bytes)))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

#[cfg(test)]
mod tests {
    use crate::object_store::s3::*;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::listing::*;
    use datafusion::datasource::TableProvider;
    use datafusion::error::Result;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use futures::StreamExt;

    const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const MINIO_ENDPOINT: &str = "http://localhost:9000";

    fn create_bucket(name: &str) -> S3FileSystem {
        // std::env::set_var("RUST_LOG", "INFO");
        // env_logger::init();

        S3FileSystem::new_custom(
            name,
            MINIO_ENDPOINT,
            Some(ACCESS_KEY_ID),
            Some(SECRET_ACCESS_KEY),
        )
        .unwrap()
    }

    // Test that `S3FileSystem` can read files
    #[tokio::test]
    async fn test_read_files() -> Result<()> {
        let s3_file_system = create_bucket("data");

        let mut files = s3_file_system.list_file("").await?;

        while let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            let mut reader = s3_file_system
                .file_reader(sized_file.clone())
                .unwrap()
                .sync_chunk_reader(0, sized_file.size as usize)
                .unwrap();

            let mut bytes = Vec::new();
            let size = reader.read_to_end(&mut bytes)?;

            assert_eq!(size as u64, sized_file.size);
        }
        Ok(())
    }
    // Test that reading files with `S3FileSystem` produces the expected results
    #[tokio::test]
    async fn test_read_range() -> Result<()> {
        let start = 10;
        let length = 128;

        let mut file = std::fs::File::open("parquet-testing/data/alltypes_plain.snappy.parquet")?;
        let mut raw_bytes = Vec::new();
        file.read_to_end(&mut raw_bytes)?;
        let raw_slice = &raw_bytes[start..start + length];
        assert_eq!(raw_slice.len(), length);

        let s3_file_system = create_bucket("data");
        let mut files = s3_file_system
            .list_file("alltypes_plain.snappy.parquet")
            .await?;

        if let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            let mut reader = s3_file_system
                .file_reader(sized_file)
                .unwrap()
                .sync_chunk_reader(start as u64, length)
                .unwrap();

            let mut reader_bytes = Vec::new();
            let size = reader.read_to_end(&mut reader_bytes)?;

            assert_eq!(size, length);
            assert_eq!(&reader_bytes, raw_slice);
        }

        Ok(())
    }

    // Test that reading Parquet file with `S3FileSystem` can create a `ListingTable`
    #[tokio::test]
    async fn test_read_parquet() -> Result<()> {
        let s3_file_system = Arc::new(create_bucket("sreport"));

        let filename = "stops.parquet";

        let config = ListingTableConfig::new(s3_file_system, filename)
            .infer()
            .await?;

        let table = ListingTable::try_new(config)?;

        let exec = table.scan(&None, &[], Some(1024)).await?;
        assert_eq!(exec.statistics().num_rows, Some(2));

        Ok(())
    }

    // Test that a SQL query can be executed on a Parquet file that was read from `S3FileSystem`
    #[tokio::test]
    async fn test_sql_query() -> Result<()> {
        let s3_file_system = Arc::new(create_bucket("data"));
        let filename = "alltypes_plain.snappy.parquet";

        let config = ListingTableConfig::new(s3_file_system, filename)
            .infer()
            .await?;

        let table = ListingTable::try_new(config)?;

        let ctx = SessionContext::new();

        ctx.register_table("tbl", Arc::new(table)).unwrap();

        let batches = ctx.sql("SELECT * FROM tbl").await?.collect().await?;
        let expected = vec![
           "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
           "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
           "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
           "| 6  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30342f30312f3039 | 30         | 2009-04-01 00:00:00 |",
           "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01 00:01:00 |",
           "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+"
           ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    // Test that the S3FileSystem allows reading from different buckets
    #[tokio::test]
    #[should_panic(expected = "Could not parse metadata: bad data")]
    async fn test_read_alternative_bucket() {
        let s3_file_system = Arc::new(create_bucket("bad_data"));

        let filename = "PARQUET-1481.parquet";

        let config = ListingTableConfig::new(s3_file_system, filename)
            .infer()
            .await
            .unwrap();

        let table = ListingTable::try_new(config).unwrap();

        table.scan(&None, &[], Some(1024)).await.unwrap();
    }

    // Test that `S3FileSystem` can be registered as object store on a DataFusion `SessionContext`
    #[tokio::test]
    async fn test_ctx_register_object_store() -> Result<()> {
        let s3_file_system = Arc::new(create_bucket("data"));

        let ctx = SessionContext::new();
        let runtime_env = ctx.runtime_env();
        runtime_env.register_object_store("s3", s3_file_system);

        let (_, name) = runtime_env.object_store("s3").unwrap();
        assert_eq!(name, "s3");

        ctx.register_parquet(
            "mytable",
            "s3://list_columns.parquet",
            ParquetReadOptions::default(),
        )
        .await?;

        let batches = ctx
            .sql("SELECT count(*) AS count FROM mytable")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+-------+",
            "| count |",
            "+-------+",
            "| 3     |",
            "+-------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    // Test that an appropriate error message is produced for a non existent bucket
    #[tokio::test]
    //#[should_panic(expected = "NoSuchBucket")]
    #[should_panic(expected = "missing field `Name`")]
    async fn test_read_nonexistent_bucket() {
        let s3_file_system = Arc::new(create_bucket("nonexistent_data"));

        let mut files = s3_file_system.list_file("").await.unwrap();

        while let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            let mut reader = s3_file_system
                .file_reader(sized_file.clone())
                .unwrap()
                .sync_chunk_reader(0, sized_file.size as usize)
                .unwrap();

            let mut bytes = Vec::new();
            let size = reader.read_to_end(&mut bytes).unwrap();

            assert_eq!(size as u64, sized_file.size);
        }
    }

    // Test that no files are returned if a non existent file URI is provided
    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let s3_file_system = Arc::new(create_bucket("data"));
        let mut files = s3_file_system
            .list_file("nonexistent_file.txt")
            .await
            .unwrap();

        assert!(files.next().await.is_none())
    }
}
