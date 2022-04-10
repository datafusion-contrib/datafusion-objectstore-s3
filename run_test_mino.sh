#!/bin/sh

set -e

git submodule update --init --recursive

docker run \
  --rm \
  --publish 9000:9000 \
  --publish 9001:9001 \
  --name minio \
  --volume "$(pwd)/parquet-testing:/data" \
  --env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
  --env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  quay.io/minio/minio server /data \
  --console-address ":9001"
