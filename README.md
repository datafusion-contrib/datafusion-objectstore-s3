# datafusion-objectstore-s3

Enable S3 as an ObjectStore for Datafusion

## Testing

Tests are run with [MinIO](https://min.io/) which provides a containerized implementation of the Amazon S3 API.

First clone the test data repository:

```bash
git clone https://github.com/apache/parquet-testing.git
```

Then start the MinIO container:

```bash
docker run \
--rm \
-p 9000:9000 \
-p 9001:9001 \
--name minio \
-v "$(pwd)/parquet-testing:/data" \
-e "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
-e "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
quay.io/minio/minio server /data \
--console-address ":9001"
```

Once started, run tests in normal fashion:

```bash
cargo test
```
