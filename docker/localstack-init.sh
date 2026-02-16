#!/bin/bash
# LocalStack initialization script — runs when LocalStack is ready.
# Creates the test bucket used by the integration test suite.

set -e

BUCKET="${TEST_BUCKET:-xs3lerator-test}"

echo "Creating S3 bucket: ${BUCKET}"
awslocal s3 mb "s3://${BUCKET}" 2>/dev/null || true

echo "LocalStack S3 initialization complete"
