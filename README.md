# datafusion-objectstore-s3

Enable S3 as an ObjectStore for Datafusion

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
