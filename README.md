# DataFusion-ObjectStore-S3

S3 as an ObjectStore for [Datafusion](https://github.com/apache/arrow-datafusion).

## Querying files on S3 with DataFusion

This crate implements the DataFusion `ObjectStore` trait on AWS S3 and implementers of the S3 standard. We leverage the official [AWS Rust SDK](https://github.com/awslabs/aws-sdk-rust) for interacting with S3. While it is our understanding that the AWS APIs we are using a relatively stable, we can make no assurances on API stability either on AWS' part or within this crate. This crates API is tightly connected with DataFusion, a fast moving project, and as such we will make changes inline with those upstream changes.

## Examples

Examples for querying AWS and other implementors, such as MinIO, are shown below.

Load credentials from default AWS credential provider (such as environment or ~/.aws/credentials)

```rust
let s3_file_system = Arc::new(S3FileSystem::default().await);
```

`S3FileSystem::default()` is a convenience wrapper for `S3FileSystem::new(None, None, None, None, None, None)`.

Connect to implementor of S3 API (MinIO, in this case) using access key and secret.

```rust
// Example credentials provided by MinIO
const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const BUCKET_NAME: &str = "data";
const MINIO_ENDPOINT: &str = "http://localhost:9000";
let s3_file_system = S3FileSystem::new_custom(
    BUCKET_NAME,
    MINIO_ENDPOINT,
    Some(MINIO_ACCESS_KEY_ID),
    Some(MINIO_SECRET_ACCESS_KEY),
   S3FileSystemOptions::from_envs()?
)?;
```

Using DataFusion's `ListingTableConfig` we register a table into a DataFusion `SessionContext` so that it can be queried.

```rust
let filename = "alltypes_plain.snappy.parquet";

let config = ListingTableConfig::new(s3_file_system, filename).infer().await?;

let table = ListingTable::try_new(config)?;

let mut ctx = SessionContext::new();

ctx.register_table("tbl", Arc::new(table))?;

let df = ctx.sql("SELECT * FROM tbl").await?;
df.show()
```

We can also register the `S3FileSystem` directly as an `ObjectStore` on an `SessionContext`. This provides an idiomatic way of creating `TableProviders` that can be queried.

```rust
execution_ctx.register_object_store(
    "s3",
    Arc::new(S3FileSystem::default().await),
);

let input_uri = "s3://alltypes_plain.snappy.parquet";

let (object_store, _) = ctx.remote_env().object_store(input_uri)?;

let config = ListingTableConfig::new(s3_file_system, input_uri).infer().await?;

let mut table_provider: Arc<dyn TableProvider + Send + Sync> = Arc::new(ListingTable::try_new(config)?);
```

## Testing

Tests are run with [MinIO](https://min.io/) which provides a containerized implementation of the Amazon S3 API.

First clone the test data repository:

```bash
git submodule update --init --recursive
```

Then start the MinIO container:

```bash
docker run \
--detach \
--rm \
--publish 9000:9000 \
--publish 9001:9001 \
--name minio \
--volume "$(pwd)/parquet-testing:/data" \
--env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
--env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
quay.io/minio/minio server /data \
--console-address ":9001"
```

Once started, run tests in normal fashion:

```bash
cargo test
```
