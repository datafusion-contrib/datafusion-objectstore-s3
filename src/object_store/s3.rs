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

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::Display;
use std::io;
use std::io::{Cursor, ErrorKind, Read};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, AsyncRead};

use datafusion_data_access::{FileMeta, Result, SizedFile};
use futures::stream::StreamExt;

use crate::object_store::cache::Cache;
use crate::object_store::parquet::load_parquet_metadata;
use crate::object_store::worker::{BucketWorker, GetObjectRange};
use datafusion_data_access::object_store::{
    FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use parking_lot::Mutex;
use s3::creds::Credentials;
use s3::{Bucket, Region};

#[derive(Debug)]
struct S3File {
    f: FileMeta,
    metadata: Option<(u64, Bytes)>,
}

#[derive(Debug, Default)]
struct S3Data {
    files: HashMap<String, S3File>,
}

/// Options for `S3FileSystem`
#[derive(Debug)]
pub struct S3FileSystemOptions {
    /// Minimal bytes request size to S3 server, extra data is used as cache
    min_request_size: usize,
    /// How jobs to use during parquet metadata pre-fetch step (during listing files)
    concurrent_jobs: usize,
    /// Maximal number of caches per file
    max_cache_branches_per_file: usize,
    /// Fetch parquet metadata for all files during listing step
    use_metadata_prefetch: bool,
}

impl Default for S3FileSystemOptions {
    fn default() -> Self {
        S3FileSystemOptions {
            min_request_size: 64 * 1024,
            concurrent_jobs: num_cpus::get(),
            max_cache_branches_per_file: 8,
            use_metadata_prefetch: true,
        }
    }
}

impl S3FileSystemOptions {
    fn parse_env<F: FromStr>(env: &str) -> Result<Option<F>>
    where
        F::Err: Display,
    {
        std::env::var(env)
            .ok()
            .map(|s| s.parse())
            .transpose()
            .map_err(|e| {
                io::Error::new(
                    ErrorKind::Other,
                    format!("cannot parse env '{}' error: {}", env, e),
                )
            })
    }

    /// Get options from envs DATAFUSION_S3_MIN_REQUEST_SIZE and DATAFUSION_S3_CONCURRENT_JOBS
    /// if evn missing, return defaults
    pub fn from_envs() -> Result<Self> {
        let default = S3FileSystemOptions::default();

        let min_request_size: usize =
            S3FileSystemOptions::parse_env("DATAFUSION_S3_MIN_REQUEST_SIZE")?
                .unwrap_or(default.min_request_size);

        let concurrent_jobs: usize =
            S3FileSystemOptions::parse_env("DATAFUSION_S3_CONCURRENT_JOBS")?
                .unwrap_or(default.concurrent_jobs);

        let max_cache_branches_per_file: usize =
            S3FileSystemOptions::parse_env("DATAFUSION_S3_MAX_CACHE_BRANCHES_PER_FILE")?
                .unwrap_or(default.max_cache_branches_per_file);

        let use_metadata_prefetch: bool =
            S3FileSystemOptions::parse_env("DATAFUSION_S3_USE_METADATA_PREFETCH")?
                .unwrap_or(default.use_metadata_prefetch);

        Ok(Self {
            min_request_size,
            concurrent_jobs,
            max_cache_branches_per_file,
            use_metadata_prefetch,
        })
    }
}

/// `ObjectStore` implementation for the Amazon S3 API
#[derive(Debug)]
pub struct S3FileSystem {
    bucket: Arc<Bucket>,
    worker_tx: Arc<mpsc::Sender<GetObjectRange>>,
    counter: AtomicUsize,
    options: S3FileSystemOptions,
    data: Mutex<S3Data>,
}

impl S3FileSystem {
    /// Create new `ObjectStore`
    pub fn new_custom(
        bucket_name: &str,
        endpoint: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        options: S3FileSystemOptions,
    ) -> Result<Self> {
        assert!(options.concurrent_jobs >= 1);

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

        let jobs_num = options.concurrent_jobs;
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(jobs_num)
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
            options,
            data: Default::default(),
        })
    }
}

fn extract_schema_and_path(prefix: &str) -> (Option<&str>, &str) {
    if let Some((scheme, path)) = prefix.split_once("://") {
        (Some(scheme), path)
    } else {
        (None, prefix)
    }
}

#[async_trait]
impl ObjectStore for S3FileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let (schema, path) = extract_schema_and_path(prefix);

        let objects = self
            .bucket
            .list(path.to_string(), None)
            .await
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?
            .into_iter()
            .flat_map(|list| list.contents)
            .map(|object| {
                Ok(FileMeta {
                    sized_file: SizedFile {
                        path: match schema {
                            Some(schema) => format!("{}://{}", schema, object.key),
                            None => object.key,
                        },
                        size: object.size,
                    },
                    last_modified: Some(
                        object
                            .last_modified
                            .parse()
                            .map_err(|e| io::Error::new(ErrorKind::Other, e))?,
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if self.options.use_metadata_prefetch {
            let futures = {
                // check which files requires pre-fetch parquet metadata
                let mut inner_data = self.data.lock();

                let mut futures = vec![];
                for o in &objects {
                    if let Some(c) = inner_data.files.get(o.path()) {
                        if c.f.last_modified == o.last_modified {
                            // file not modified, skipping
                            continue;
                        }
                    }

                    if !o.path().ends_with(".parquet") {
                        // not parquet file
                        inner_data.files.insert(
                            o.path().to_string(),
                            S3File {
                                f: o.clone(),
                                metadata: None,
                            },
                        );
                        continue;
                    }

                    // create future to pre-fetch metadata cache for file
                    let bucket = self.bucket.clone();
                    let o = o.clone();
                    futures.push(async move {
                        log::debug!("loading metadata for: {o:?}");
                        let (_schema, path) = extract_schema_and_path(o.path());
                        let r = load_parquet_metadata(&bucket, path, o.size()).await;
                        r.map(|(start, metadata)| (o, start, metadata))
                    });
                }

                futures
            };

            let stream =
                futures::stream::iter(futures).buffer_unordered(self.options.concurrent_jobs);
            let results = stream.collect::<Vec<_>>().await;

            {
                let mut inner_data = self.data.lock();

                for r in results {
                    let (o, start, bytes) = r?;

                    inner_data.files.insert(
                        o.path().to_string(),
                        S3File {
                            f: o,
                            metadata: Some((start, bytes)),
                        },
                    );
                }
            }
        }

        let result = stream::iter(objects.into_iter().map(Result::Ok));
        Ok(Box::pin(result))
    }

    async fn list_dir(&self, _prefix: &str, _delimiter: Option<String>) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        let id = self.counter.fetch_add(1, Ordering::Acquire);

        let cache = Cache::new(self.options.max_cache_branches_per_file);

        let inner_data = self.data.lock();
        if let Some(cached_file) = inner_data.files.get(&file.path) {
            if let Some((start, metadata)) = cached_file.metadata.clone() {
                cache.put(start, metadata);
            }
        }

        Ok(Arc::new(S3CompatibleFileReader {
            id,
            path: file.path,
            file_size: file.size,
            worker_tx: self.worker_tx.clone(),
            cache,
            min_request_size: self.options.min_request_size,
        }))
    }
}

struct S3CompatibleFileReader {
    id: usize,
    path: String,
    file_size: u64,
    worker_tx: Arc<mpsc::Sender<GetObjectRange>>,
    cache: Cache,
    min_request_size: usize,
}

impl S3CompatibleFileReader {}

#[async_trait]
impl ObjectReader for S3CompatibleFileReader {
    async fn chunk_reader(&self, _start: u64, _length: usize) -> Result<Box<dyn AsyncRead>> {
        todo!("implement once async file readers are available (arrow-rs#78, arrow-rs#111)")
    }

    fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>> {
        if let Some(bytes) = self.cache.get(start, length) {
            return Ok(Box::new(Cursor::new(bytes)));
        }

        let req_length = min(
            max(length, self.min_request_size) as u64,
            self.file_size - start,
        ) as usize;
        log::debug!(
            "direct id: {} file: {} start: {} len: {}, rlen: {}",
            self.id,
            self.path,
            start,
            length,
            req_length
        );

        let (_schema, path) = extract_schema_and_path(&self.path);
        let (tx, rx) = std::sync::mpsc::channel();
        let req = GetObjectRange {
            id: self.id,
            path: path.to_string(),
            start,
            length: req_length,
            tx,
        };

        self.worker_tx.try_send(req).unwrap();

        let bytes = rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|e| io::Error::new(ErrorKind::Other, e))??;

        // save to cache
        let r = if bytes.len() <= req_length {
            self.cache.put(start, bytes.clone());
            bytes.slice(..length)
        } else {
            bytes
        };

        Ok(Box::new(Cursor::new(r)))
    }

    fn length(&self) -> u64 {
        self.file_size
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
    use test_log::test;

    const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const MINIO_ENDPOINT: &str = "http://localhost:9000";

    fn create_bucket(name: &str) -> S3FileSystem {
        let options = S3FileSystemOptions::default();

        S3FileSystem::new_custom(
            name,
            MINIO_ENDPOINT,
            Some(ACCESS_KEY_ID),
            Some(SECRET_ACCESS_KEY),
            options,
        )
        .unwrap()
    }

    // Test that `S3FileSystem` can read files
    #[test(tokio::test)]
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
    #[test(tokio::test)]
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
    #[test(tokio::test)]
    async fn test_read_parquet() -> Result<()> {
        let s3_file_system = Arc::new(create_bucket("data"));

        let filename = "alltypes_plain.snappy.parquet";

        let config = ListingTableConfig::new(s3_file_system, filename)
            .infer()
            .await?;

        let table = ListingTable::try_new(config)?;

        let exec = table.scan(&None, &[], Some(1024)).await?;
        assert_eq!(exec.statistics().num_rows, Some(2));

        Ok(())
    }

    // Test that a SQL query can be executed on a Parquet file that was read from `S3FileSystem`
    #[test(tokio::test)]
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
    #[test(tokio::test)]
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
    #[test(tokio::test)]
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
    #[test(tokio::test)]
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
    #[test(tokio::test)]
    async fn test_read_nonexistent_file() {
        let s3_file_system = Arc::new(create_bucket("data"));
        let mut files = s3_file_system
            .list_file("nonexistent_file.txt")
            .await
            .unwrap();

        assert!(files.next().await.is_none())
    }
}
