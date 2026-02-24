#!/bin/sh
# Wait for LocalStack S3 to be ready, then mount the bucket via mount-s3.
# Used as the entrypoint for the mount-s3 sidecar in docker-compose.
set -e

BUCKET="${MOUNT_S3_BUCKET:-xs3lerator-test}"
MOUNTPOINT="${MOUNT_S3_MOUNTPOINT:-/data}"
ENDPOINT="${MOUNT_S3_ENDPOINT:-http://localstack:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "Waiting for S3 endpoint at ${ENDPOINT}..."
until wget -qO- "${ENDPOINT}/_localstack/health" >/dev/null 2>&1; do
    sleep 1
done
echo "S3 endpoint ready."

echo "Mounting s3://${BUCKET} to ${MOUNTPOINT}"
mkdir -p "${MOUNTPOINT}"
exec mount-s3 \
    --endpoint-url "${ENDPOINT}" \
    --region "${REGION}" \
    --allow-delete \
    --allow-other \
    --force-path-style \
    -f \
    "${BUCKET}" "${MOUNTPOINT}"
