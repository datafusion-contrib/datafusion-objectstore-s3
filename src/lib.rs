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

#![warn(missing_docs)]

//! [DataFusion-ObjectStore-S3](https://github.com/datafusion-contrib/datafusion-objectstore-s3)
//! provides a `TableProvider` interface for using `Datafusion` to query data in S3.  This includes AWS S3
//! and services such as MinIO that implement the S3 API.
//!
//! The initial implementation was donated by [Mike Seddon](https://github.com/seddonm1).
//!
//! ## Examples
//! Examples for querying AWS and other implementors, such as MinIO, are shown below.
//!
//! Load credentials from default AWS credential provider (such as environment or ~/.aws/credentials)
//!
//! ```rust
//! # use std::sync::Arc;
//! # use datafusion::error::Result;
//! # use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let s3_file_system = Arc::new(S3FileSystem::default().await);
//! # Ok(())
//! # }
//! ```
//!
//! `S3FileSystem::default()` is a convenience wrapper for `S3FileSystem::new(None, None, None, None, None, None)`.
//!
//! Connect to implementor of S3 API (MinIO, in this case) use access key and secret.
//!
//! ```rust
//! use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
//! use aws_types::credentials::SharedCredentialsProvider;
//! use aws_types::credentials::Credentials;
//! use aws_sdk_s3::Endpoint;
//! use http::Uri;
//!
//! # #[tokio::main]
//! # async fn main() {
//! // Example credentials provided by MinIO
//! const MINIO_ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
//! const MINIO_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
//! const PROVIDER_NAME: &str = "Static";
//! const MINIO_ENDPOINT: &str = "http://localhost:9000";
//!
//! let s3_file_system = S3FileSystem::new(
//!     Some(SharedCredentialsProvider::new(Credentials::new(
//!         MINIO_ACCESS_KEY_ID,
//!         MINIO_SECRET_ACCESS_KEY,
//!         None,
//!         None,
//!         PROVIDER_NAME,
//!     ))),
//!     None,
//!     Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
//!     None,
//!     None,
//!     None,
//! )
//! .await;
//! # }
//! ```
//!
//! Using DataFusion's `ListingOtions` and `ListingTable` we register a table into a DataFusion `ExecutionContext` so that it can be queried.
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use datafusion::datasource::listing::*;
//! use datafusion::datasource::TableProvider;
//! use datafusion::prelude::ExecutionContext;
//! use datafusion::datasource::file_format::parquet::ParquetFormat;
//! use datafusion::error::Result;
//!
//! use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
//!
//! use aws_types::credentials::SharedCredentialsProvider;
//! use aws_types::credentials::Credentials;
//! use aws_sdk_s3::Endpoint;
//! use http::Uri;
//!
//! # const MINIO_ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
//! # const MINIO_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
//! # const PROVIDER_NAME: &str = "Static";
//! # const MINIO_ENDPOINT: &str = "http://localhost:9000";
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let filename = "data/alltypes_plain.snappy.parquet";
//!
//! let listing_options = ListingOptions {
//!     format: Arc::new(ParquetFormat::default()),
//!     collect_stat: true,
//!     file_extension: "parquet".to_owned(),
//!     target_partitions: num_cpus::get(),
//!     table_partition_cols: vec![],
//! };
//!
//! # let s3_file_system = Arc::new(S3FileSystem::new(
//! #     Some(SharedCredentialsProvider::new(Credentials::new(
//! #         MINIO_ACCESS_KEY_ID,
//! #         MINIO_SECRET_ACCESS_KEY,
//! #         None,
//! #         None,
//! #         PROVIDER_NAME,
//! #     ))),
//! #     None,
//! #     Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
//! #     None,
//! #     None,
//! #     None,
//! # )
//! # .await);
//!
//! let resolved_schema = listing_options
//!     .infer_schema(s3_file_system.clone(), filename)
//!     .await?;
//!
//! let table = ListingTable::new(
//!     s3_file_system,
//!     filename.to_owned(),
//!     resolved_schema,
//!     listing_options,
//! );
//!
//! let mut ctx = ExecutionContext::new();
//!
//! ctx.register_table("tbl", Arc::new(table))?;
//!
//! let df = ctx.sql("SELECT * FROM tbl").await?;
//! df.show();
//! # Ok(())
//! # }
//! ```
//!
//! We can also register the `S3FileSystem` directly as an `ObjectStore` on an `ExecutionContext`. This provides an idiomatic way of creating `TableProviders` that can be queried.
//!
//! ```rust
//! # use std::sync::Arc;
//! # use datafusion::datasource::listing::*;
//! # use datafusion::datasource::TableProvider;
//! # use datafusion::prelude::ExecutionContext;
//! # use datafusion::datasource::file_format::parquet::ParquetFormat;
//! # use datafusion::datasource::object_store::ObjectStore;
//! # use datafusion::error::Result;
//! # use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
//! # use aws_types::credentials::SharedCredentialsProvider;
//! # use aws_types::credentials::Credentials;
//! # use aws_sdk_s3::Endpoint;
//! # use http::Uri;
//! # const MINIO_ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
//! # const MINIO_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
//! # const PROVIDER_NAME: &str = "Static";
//! # const MINIO_ENDPOINT: &str = "http://localhost:9000";
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let mut ctx = ExecutionContext::new();
//! let s3_file_system = S3FileSystem::new(
//!     Some(SharedCredentialsProvider::new(Credentials::new(
//!         MINIO_ACCESS_KEY_ID,
//!         MINIO_SECRET_ACCESS_KEY,
//!         None,
//!         None,
//!         PROVIDER_NAME,
//!     ))),
//!     None,
//!     Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
//!     None,
//!     None,
//!     None,
//! )
//! .await;
//! ctx.register_object_store("s3", Arc::new(s3_file_system));
//!
//! let input_uri = "data/alltypes_plain.snappy.parquet";
//!
//! let (object_store, _) = ctx.object_store("s3")?;
//!
//! let listing_options = ListingOptions {
//!     format: Arc::new(ParquetFormat::default()),
//!     collect_stat: true,
//!     file_extension: "parquet".to_owned(),
//!     target_partitions: num_cpus::get(),
//!     table_partition_cols: vec![],
//! };
//!
//! let resolved_schema = listing_options
//!     .infer_schema(object_store.clone(), input_uri)
//!     .await?;
//!
//! let mut table_provider: Arc<dyn TableProvider + Send + Sync> = Arc::new(ListingTable::new(
//!     object_store,
//!     input_uri.to_string(),
//!     resolved_schema,
//!     listing_options,
//! ));
//! # Ok(())
//! # }
//! ```
//!

pub mod error;
pub mod object_store;
