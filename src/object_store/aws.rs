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

use std::io::Read;
use std::str::FromStr;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use async_trait::async_trait;
use futures::{stream, AsyncRead};

use datafusion::datasource::object_store::SizedFile;
use datafusion::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::error::DataFusionError;
use datafusion::error::Result;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Config, Endpoint, Region, RetryConfig};
use aws_smithy_types::timeout::TimeoutConfig;
use aws_smithy_types_convert::date_time::DateTimeExt;
use aws_types::credentials::Credentials;
use bytes::Buf;
use http::Uri;

pub struct S3Context {
    region: Option<String>,
    endpoint: Option<String>,
    retry_max_attempts: Option<u32>,
    api_call_attempt_timeout_seconds: Option<u64>,
    access_key: Option<String>,
    secret_key: Option<String>,
}

impl S3Context {
    async fn to_client(self) -> Client {
        let S3Context {
            region,
            endpoint,
            retry_max_attempts,
            api_call_attempt_timeout_seconds,
            access_key,
            secret_key,
        } = self;

        let region_provider = RegionProviderChain::first_try(region.map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-west-2"))
            .region()
            .await;

        let mut config_builder = Config::builder().region(region_provider);

        if let Some(endpoint) = endpoint {
            config_builder = config_builder
                .endpoint_resolver(Endpoint::immutable(Uri::from_str(&endpoint).unwrap()));
        }

        if let Some(retry_max_attempts) = retry_max_attempts {
            config_builder = config_builder
                .retry_config(RetryConfig::new().with_max_attempts(retry_max_attempts));
        }

        if let Some(api_call_attempt_timeout_seconds) = api_call_attempt_timeout_seconds {
            config_builder =
                config_builder.timeout_config(TimeoutConfig::new().with_api_call_attempt_timeout(
                    Some(Duration::from_secs(api_call_attempt_timeout_seconds)),
                ));
        };

        if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
            config_builder = config_builder.credentials_provider(Credentials::new(
                access_key, secret_key, None, None, "Static",
            ));
        };

        let config = config_builder.build();
        Client::from_conf(config)
    }
}

/// Holds all s3 authorization variants
enum Authorizer {
    Environment,
    Http(S3Context),
}

impl Authorizer {
    async fn authorize(self) -> Client {
        match self {
            Authorizer::Environment => {
                let config = aws_config::load_from_env().await;
                Client::new(&config)
            }
            Authorizer::Http(ctx) => ctx.to_client().await,
        }
    }
}

/// new_client creates a new aws_sdk_s3::Client
/// at time of writing the aws_config::load_from_env() does not allow configuring the endpoint which is
/// required for continuous integration testing and uses outside the AWS ecosystem
async fn new_client(authorizer: Authorizer) -> Client {
    authorizer.authorize().await
}

#[derive(Debug)]
// ObjectStore implementation for the Amazon S3 API
pub struct AmazonS3FileSystem {
    region: Option<String>,
    endpoint: Option<String>,
    retry_max_attempts: Option<u32>,
    api_call_attempt_timeout_seconds: Option<u64>,
    access_key: Option<String>,
    secret_key: Option<String>,
    bucket: String,
    client: Client,
}

impl AmazonS3FileSystem {
    pub async fn new(
        // region: Option<String>,
        // endpoint: Option<String>,
        // retry_max_attempts: Option<u32>,
        // api_call_attempt_timeout_seconds: Option<u64>,
        // access_key: Option<String>,
        // secret_key: Option<String>,
        authorizer: Authorizer,
        bucket: &str,
    ) -> Self {
        let client = new_client(authorizer).await;

        Self {
            region,
            endpoint,
            retry_max_attempts,
            api_call_attempt_timeout_seconds,
            access_key,
            secret_key,
            bucket: bucket.to_string(),
            client,
        }
    }
}

#[async_trait]
impl ObjectStore for AmazonS3FileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let objects = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .send()
            .await
            .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?
            .contents()
            .unwrap_or_default()
            .to_vec();

        let result = stream::iter(objects.into_iter().map(|object| {
            Ok(FileMeta {
                sized_file: SizedFile {
                    path: object.key().unwrap_or("").to_string(),
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
            self.region.clone(),
            self.endpoint.clone(),
            self.retry_max_attempts,
            self.api_call_attempt_timeout_seconds,
            self.access_key.clone(),
            self.secret_key.clone(),
            &self.bucket,
            file,
        )?))
    }
}

struct AmazonS3FileReader {
    region: Option<String>,
    endpoint: Option<String>,
    retry_max_attempts: Option<u32>,
    api_call_attempt_timeout_seconds: Option<u64>,
    access_key: Option<String>,
    secret_key: Option<String>,
    bucket: String,
    file: SizedFile,
}

impl AmazonS3FileReader {
    #[allow(clippy::too_many_arguments)]
    fn new(
        region: Option<String>,
        endpoint: Option<String>,
        retry_max_attempts: Option<u32>,
        api_call_attempt_timeout_seconds: Option<u64>,
        access_key: Option<String>,
        secret_key: Option<String>,
        bucket: &str,
        file: SizedFile,
    ) -> Result<Self> {
        Ok(Self {
            region,
            endpoint,
            retry_max_attempts,
            api_call_attempt_timeout_seconds,
            access_key,
            secret_key,
            bucket: bucket.to_string(),
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
        let region = self.region.clone();
        let endpoint = self.endpoint.clone();
        let retry_max_attempts = self.retry_max_attempts;
        let api_call_attempt_timeout_seconds = self.api_call_attempt_timeout_seconds;
        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let bucket = self.bucket.clone();
        let key = self.file.path.clone();

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
                    region,
                    endpoint,
                    retry_max_attempts,
                    api_call_attempt_timeout_seconds,
                    access_key,
                    secret_key,
                )
                .await;

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
                            Err(err) => Err(DataFusionError::Internal(format!("{:?}", err))),
                        }
                    }
                    Err(err) => Err(DataFusionError::Internal(format!("{:?}", err))),
                };

                tx.send(bytes).unwrap();
            })
        });

        let bytes = rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))??;

        Ok(Box::new(bytes.reader()))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::*;
    use datafusion::datasource::TableProvider;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_read_files() -> Result<()> {
        let amazon_s3_file_system = AmazonS3FileSystem::new(
            None,
            Some("http://localhost:9000".to_string()),
            None,
            None,
            Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "data",
            false,
        )
        .await;
        let mut files = amazon_s3_file_system.list_file("").await?;

        while let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            let mut reader = amazon_s3_file_system
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

    #[tokio::test]
    async fn test_read_range() -> Result<()> {
        let start = 10;
        let length = 128;

        let mut file = std::fs::File::open("parquet-testing/data/alltypes_plain.snappy.parquet")?;
        let mut raw_bytes = Vec::new();
        file.read_to_end(&mut raw_bytes)?;
        let raw_slice = &raw_bytes[start..start + length];
        assert_eq!(raw_slice.len(), length);

        let amazon_s3_file_system = AmazonS3FileSystem::new(
            None,
            Some("http://localhost:9000".to_string()),
            None,
            None,
            Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "data",
            false,
        )
        .await;
        let mut files = amazon_s3_file_system
            .list_file("alltypes_plain.snappy.parquet")
            .await?;

        if let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            let mut reader = amazon_s3_file_system
                .file_reader(sized_file.clone())
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

    #[tokio::test]
    async fn test_read_parquet() -> Result<()> {
        let amazon_s3_file_system = Arc::new(
            AmazonS3FileSystem::new(
                None,
                Some("http://localhost:9000".to_string()),
                None,
                None,
                Some("AKIAIOSFODNN7EXAMPLE".to_string()),
                Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
                "data",
                false,
            )
            .await,
        );

        let filename = "alltypes_plain.snappy.parquet";

        let listing_options = ListingOptions {
            format: Arc::new(ParquetFormat::default()),
            collect_stat: true,
            file_extension: "parquet".to_owned(),
            target_partitions: num_cpus::get(),
            table_partition_cols: vec![],
        };

        let resolved_schema = listing_options
            .infer_schema(amazon_s3_file_system.clone(), filename)
            .await?;

        let table = ListingTable::new(
            amazon_s3_file_system,
            filename.to_owned(),
            resolved_schema,
            listing_options,
        );

        let exec = table.scan(&None, 1024, &[], None).await?;
        assert_eq!(exec.statistics().num_rows, Some(2));

        Ok(())
    }
}
