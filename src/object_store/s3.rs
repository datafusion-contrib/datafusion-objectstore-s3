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

use std::io::{ErrorKind, Read};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use async_trait::async_trait;
use futures::{stream, AsyncRead};

use datafusion::datafusion_data_access::object_store::{
    FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::datafusion_data_access::{FileMeta, Result, SizedFile};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Builder, Client, Endpoint, Region, RetryConfig};
use aws_smithy_async::rt::sleep::AsyncSleep;
use aws_smithy_types::timeout::Config;
use aws_smithy_types_convert::date_time::DateTimeExt;
use aws_types::credentials::SharedCredentialsProvider;
use bytes::Buf;

use crate::error::S3Error;

/// new_client creates a new aws_sdk_s3::Client
/// this uses aws_config::load_from_env() as a base config then allows users to override specific settings if required
///
/// an example use case for overriding is to specify an endpoint which is not Amazon S3 such as MinIO or Ceph.
async fn new_client(
    credentials_provider: Option<SharedCredentialsProvider>,
    region: Option<Region>,
    endpoint: Option<Endpoint>,
    retry_config: Option<RetryConfig>,
    sleep: Option<Arc<dyn AsyncSleep>>,
    timeout_config: Option<Config>,
) -> Client {
    let config = aws_config::load_from_env().await;

    let region_provider = RegionProviderChain::first_try(region)
        .or_default_provider()
        .or_else(Region::new("us-west-2"));

    let mut config_builder = Builder::from(&config).region(region_provider.region().await);

    if let Some(credentials_provider) = credentials_provider {
        config_builder = config_builder.credentials_provider(credentials_provider);
    }

    if let Some(endpoint) = endpoint {
        config_builder = config_builder.endpoint_resolver(endpoint);
    }

    if let Some(retry_config) = retry_config {
        config_builder = config_builder.retry_config(retry_config);
    }

    if let Some(sleep) = sleep {
        config_builder = config_builder.sleep_impl(sleep);
    }

    if let Some(timeout_config) = timeout_config {
        config_builder = config_builder.timeout_config(timeout_config);
    };

    let config = config_builder.build();
    Client::from_conf(config)
}

/// `ObjectStore` implementation for the Amazon S3 API
#[derive(Debug)]
pub struct S3FileSystem {
    credentials_provider: Option<SharedCredentialsProvider>,
    region: Option<Region>,
    endpoint: Option<Endpoint>,
    retry_config: Option<RetryConfig>,
    sleep: Option<Arc<dyn AsyncSleep>>,
    timeout_config: Option<Config>,
    client: Client,
}

impl S3FileSystem {
    /// Create new `ObjectStore`
    pub async fn new(
        credentials_provider: Option<SharedCredentialsProvider>,
        region: Option<Region>,
        endpoint: Option<Endpoint>,
        retry_config: Option<RetryConfig>,
        sleep: Option<Arc<dyn AsyncSleep>>,
        timeout_config: Option<Config>,
    ) -> Self {
        Self {
            credentials_provider: credentials_provider.clone(),
            region: region.clone(),
            endpoint: endpoint.clone(),
            retry_config: retry_config.clone(),
            sleep: sleep.clone(),
            timeout_config: timeout_config.clone(),
            client: new_client(credentials_provider, region, endpoint, None, None, None).await,
        }
    }
}

#[async_trait]
impl ObjectStore for S3FileSystem {
    async fn list_file(&self, uri: &str) -> Result<FileMetaStream> {
        let prefix = if let Some(("s3", path)) = uri.split_once("://") {
            path
        } else {
            uri
        };

        let (bucket, prefix) = match prefix.split_once('/') {
            Some((bucket, prefix)) => (bucket.to_owned(), prefix),
            None => (prefix.to_owned(), ""),
        };

        let objects = self
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(prefix)
            .send()
            .await
            .map_err(|err| {
                std::io::Error::new(ErrorKind::Other, S3Error::AWS(format!("{:?}", err)))
            })?
            .contents()
            .unwrap_or_default()
            .to_vec();

        let result = stream::iter(objects.into_iter().map(move |object| {
            Ok(FileMeta {
                sized_file: SizedFile {
                    path: format!("{}/{}", &bucket, object.key().unwrap_or("")),
                    size: object.size() as u64,
                },
                last_modified: object
                    .last_modified()
                    .map(|last_modified| last_modified.to_chrono_utc()),
            })
        }));

        Ok(Box::pin(result))
    }

    async fn list_dir(&self, _prefix: &str, _delimiter: Option<String>) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(AmazonS3FileReader::new(
            self.credentials_provider.clone(),
            self.region.clone(),
            self.endpoint.clone(),
            self.retry_config.clone(),
            self.sleep.clone(),
            self.timeout_config.clone(),
            file,
        )?))
    }
}

#[allow(dead_code)]
impl S3FileSystem {
    /// Convenience wrapper for creating a new `S3FileSystem` using default configuration options.  Only works with AWS.
    pub async fn default() -> Self {
        S3FileSystem::new(None, None, None, None, None, None).await
    }
}

struct AmazonS3FileReader {
    credentials_provider: Option<SharedCredentialsProvider>,
    region: Option<Region>,
    endpoint: Option<Endpoint>,
    retry_config: Option<RetryConfig>,
    sleep: Option<Arc<dyn AsyncSleep>>,
    timeout_config: Option<Config>,
    file: SizedFile,
}

impl AmazonS3FileReader {
    #[allow(clippy::too_many_arguments)]
    fn new(
        credentials_provider: Option<SharedCredentialsProvider>,
        region: Option<Region>,
        endpoint: Option<Endpoint>,
        retry_config: Option<RetryConfig>,
        sleep: Option<Arc<dyn AsyncSleep>>,
        timeout_config: Option<Config>,
        file: SizedFile,
    ) -> Result<Self> {
        Ok(Self {
            credentials_provider,
            region,
            endpoint,
            retry_config,
            sleep,
            timeout_config,
            file,
        })
    }
}

#[async_trait]
impl ObjectReader for AmazonS3FileReader {
    async fn chunk_reader(&self, _start: u64, _length: usize) -> Result<Box<dyn AsyncRead>> {
        todo!("implement once async file readers are available (arrow-rs#78, arrow-rs#111)")
    }

    fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>> {
        let credentials_provider = self.credentials_provider.clone();
        let region = self.region.clone();
        let endpoint = self.endpoint.clone();
        let retry_config = self.retry_config.clone();
        let sleep = self.sleep.clone();
        let timeout_config = self.timeout_config.clone();
        let file_path = self.file.path.clone();

        // once the async chunk file readers have been implemented this complexity can be removed
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                // aws_sdk_s3::Client appears bound to the runtime and will deadlock if cloned from the main runtime
                let client = new_client(
                    credentials_provider,
                    region,
                    endpoint,
                    retry_config,
                    sleep,
                    timeout_config,
                )
                .await;

                let (bucket, key) = match file_path.split_once('/') {
                    Some((bucket, prefix)) => (bucket, prefix),
                    None => (file_path.as_str(), ""),
                };

                let get_object = client.get_object().bucket(bucket).key(key);
                let resp = if length > 0 {
                    // range bytes requests are inclusive
                    get_object
                        .range(format!("bytes={}-{}", start, start + (length - 1) as u64))
                        .send()
                        .await
                } else {
                    get_object.send().await
                };

                let bytes = match resp {
                    Ok(res) => {
                        let data = res.body.collect().await;
                        match data {
                            Ok(data) => Ok(data.into_bytes()),
                            Err(err) => Err(std::io::Error::new(
                                ErrorKind::Other,
                                S3Error::AWS(format!("{:?}", err)),
                            )),
                        }
                    }
                    Err(err) => Err(std::io::Error::new(
                        ErrorKind::Other,
                        S3Error::AWS(format!("{:?}", err)),
                    )),
                };

                tx.send(bytes).unwrap();
            })
        });

        let bytes = rx.recv_timeout(Duration::from_secs(10)).map_err(|err| {
            std::io::Error::new(ErrorKind::TimedOut, S3Error::AWS(format!("{:?}", err)))
        })??;

        Ok(Box::new(bytes.reader()))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}
#[cfg(test)]
mod tests {
    use crate::object_store::s3::*;
    use aws_types::credentials::Credentials;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::listing::*;
    use datafusion::datasource::TableProvider;
    use datafusion::error::DataFusionError;
    use datafusion::prelude::*;
    use futures::StreamExt;
    use http::Uri;

    const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const PROVIDER_NAME: &str = "Static";
    const MINIO_ENDPOINT: &str = "http://localhost:9000";

    // Test that `S3FileSystem` can read files
    #[tokio::test]
    async fn test_read_files() -> Result<()> {
        let s3_file_system = S3FileSystem::new(
            Some(SharedCredentialsProvider::new(Credentials::new(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY,
                None,
                None,
                PROVIDER_NAME,
            ))),
            None,
            Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
            None,
            None,
            None,
        )
        .await;

        let mut files = s3_file_system.list_file("s3://data").await?;

        let mut files_handled = 0;
        while let Some(file) = files.next().await {
            files_handled += 1;
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
        assert!(files_handled > 0);

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

        let s3_file_system = S3FileSystem::new(
            Some(SharedCredentialsProvider::new(Credentials::new(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY,
                None,
                None,
                PROVIDER_NAME,
            ))),
            None,
            Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
            None,
            None,
            None,
        )
        .await;
        let mut files = s3_file_system
            .list_file("s3://data/alltypes_plain.snappy.parquet")
            .await?;

        let mut files_handled = 0;
        if let Some(file) = files.next().await {
            files_handled += 1;
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
        assert!(files_handled > 0);

        Ok(())
    }

    fn map_datafusion_error_to_io_error(err: DataFusionError) -> std::io::Error {
        std::io::Error::new(ErrorKind::Other, S3Error::AWS(format!("{:?}", err)))
    }

    // Test that reading Parquet file with `S3FileSystem` can create a `ListingTable`
    #[tokio::test]
    async fn test_read_parquet() -> Result<()> {

        let ctx = SessionContext::new();

        let s3_file_system = Arc::new(
            S3FileSystem::new(
                Some(SharedCredentialsProvider::new(Credentials::new(
                    ACCESS_KEY_ID,
                    SECRET_ACCESS_KEY,
                    None,
                    None,
                    PROVIDER_NAME,
                ))),
                None,
                Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
                None,
                None,
                None,
            )
            .await,
        );

        ctx.runtime_env().register_object_store("s3", s3_file_system);

        let filename = "s3://data/alltypes_plain.snappy.parquet";

        let config = ListingTableConfig::new(ListingTableUrl::parse(filename).map_err(map_datafusion_error_to_io_error)?)
            .infer(&ctx.state())
            .await
            .map_err(map_datafusion_error_to_io_error)?;

        let table = ListingTable::try_new(config).map_err(map_datafusion_error_to_io_error)?;

        let exec = table
            .scan(&ctx.state(), &None,&[], Some(1024))
            .await
            .map_err(map_datafusion_error_to_io_error)?;
        assert_eq!(exec.statistics().num_rows, Some(2));

        Ok(())
    }

    // Test that a SQL query can be executed on a Parquet file that was read from `S3FileSystem`
    #[tokio::test]
    async fn test_sql_query() -> Result<()> {
        let ctx = SessionContext::new();

        let s3_file_system = Arc::new(
            S3FileSystem::new(
                Some(SharedCredentialsProvider::new(Credentials::new(
                    ACCESS_KEY_ID,
                    SECRET_ACCESS_KEY,
                    None,
                    None,
                    PROVIDER_NAME,
                ))),
                None,
                Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
                None,
                None,
                None,
            )
            .await,
        );

        ctx.runtime_env().register_object_store("s3", s3_file_system);

        let filename = "s3://data/alltypes_plain.snappy.parquet";

        let config = ListingTableConfig::new(ListingTableUrl::parse(filename).unwrap())
            .infer(&ctx.state())
            .await
            .unwrap();

        let table = ListingTable::try_new(config).unwrap();

        ctx.register_table("tbl", Arc::new(table)).unwrap();

        let batches = ctx
            .sql("SELECT * FROM tbl")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

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

    // Test that a SQL query can be executed on a Parquet file that was read from `S3FileSystem`
    #[tokio::test]
    async fn test_create_external_table_sql_query() -> Result<()> {

        let ctx = SessionContext::new();

        let s3_file_system = Arc::new(
            S3FileSystem::new(
                Some(SharedCredentialsProvider::new(Credentials::new(
                    ACCESS_KEY_ID,
                    SECRET_ACCESS_KEY,
                    None,
                    None,
                    PROVIDER_NAME,
                ))),
                None,
                Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
                None,
                None,
                None,
            )
            .await,
        );

        ctx.runtime_env().register_object_store("s3", s3_file_system);

        let sql = "CREATE EXTERNAL TABLE abc STORED AS PARQUET LOCATION 's3://data/alltypes_plain.snappy.parquet'";

        ctx.sql(sql).await.unwrap().collect().await.unwrap();

        ctx.table("abc").unwrap();
        Ok(())
    }

    // Test that the S3FileSystem allows reading from different buckets
    #[tokio::test]
    #[should_panic(expected = "Could not parse metadata: bad data")]
    async fn test_read_alternative_bucket() {

        let s3_file_system = Arc::new(
            S3FileSystem::new(
                Some(SharedCredentialsProvider::new(Credentials::new(
                    ACCESS_KEY_ID,
                    SECRET_ACCESS_KEY,
                    None,
                    None,
                    PROVIDER_NAME,
                ))),
                None,
                Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
                None,
                None,
                None,
            )
            .await,
        );

        let ctx = SessionContext::new();
        ctx.runtime_env().register_object_store("s3", s3_file_system);

        let filename = "s3://bad_data/PARQUET-1481.parquet";

        let config = ListingTableConfig::new(ListingTableUrl::parse(filename).unwrap())
            .infer(&ctx.state())
            .await
            .unwrap();

        let table = ListingTable::try_new(config).unwrap();

        table.scan(&ctx.state(),&None, &[], Some(1024)).await.unwrap();
    }

    // Test that without registration there is no s3 filesystem on a DataFusion `ExecutionContext`
    #[tokio::test]
    async fn test_ctx_object_store() -> Result<()> {
        let ctx = SessionContext::new();
        let url = ListingTableUrl::parse("s3://").unwrap();
        assert!(ctx.runtime_env().object_store(url).is_err());
        Ok(())
    }

    // Test that `S3FileSystem` can be registered as object store on a DataFusion `ExecutionContext`
    #[tokio::test]
    async fn test_ctx_register_object_store() -> Result<()> {
        let s3_file_system = Arc::new(
            S3FileSystem::new(
                Some(SharedCredentialsProvider::new(Credentials::new(
                    ACCESS_KEY_ID,
                    SECRET_ACCESS_KEY,
                    None,
                    None,
                    PROVIDER_NAME,
                ))),
                None,
                Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
                None,
                None,
                None,
            )
            .await,
        );

        let ctx = SessionContext::new();
        ctx.runtime_env().register_object_store("s3", s3_file_system);

        let url = ListingTableUrl::parse("s3://").unwrap();
        assert!(ctx.runtime_env().object_store(url).is_ok());

        Ok(())
    }

    // Test that an appropriate error message is produced for a non existent bucket
    #[tokio::test]
    #[should_panic(expected = "NoSuchBucket")]
    async fn test_read_nonexistent_bucket() {
        let s3_file_system = S3FileSystem::new(
            Some(SharedCredentialsProvider::new(Credentials::new(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY,
                None,
                None,
                PROVIDER_NAME,
            ))),
            None,
            Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
            None,
            None,
            None,
        )
        .await;

        let mut files = s3_file_system
            .list_file("s3://nonexistent_data")
            .await
            .unwrap();

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
        let s3_file_system = S3FileSystem::new(
            Some(SharedCredentialsProvider::new(Credentials::new(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY,
                None,
                None,
                PROVIDER_NAME,
            ))),
            None,
            Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
            None,
            None,
            None,
        )
        .await;
        let mut files = s3_file_system
            .list_file("s3://data/nonexistent_file.txt")
            .await
            .unwrap();

        assert!(files.next().await.is_none())
    }
}
