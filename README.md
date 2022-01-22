# datafusion-objectstore-s3

Enable S3 as an ObjectStore for Datafusion

## Querying files on S3 with DataFusion

```rust

// Load from default credential provider
let amazon_s3_file_system = Arc::new(
    AmazonS3FileSystem::new(
        None,
        None,
        None,
        None,
        None,
        None,
        BUCKET,
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

let mut ctx = ExecutionContext::new();

ctx.register_table("tbl", Arc::new(table))?;

let df = ctx.sql("SELECT * FROM tbl").await?;
df.show()
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
