#!/usr/bin/env bash
# Tear down all CloudFormation stacks in reverse dependency order.
# Empties S3 buckets before deleting stacks.
# Usage: ./teardown-all.sh [dev|prod]
set -euo pipefail

ENVIRONMENT="${1:-dev}"
PROJECT="stock-market-pipeline"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "============================================="
echo "WARNING: This will DESTROY all ${PROJECT}"
echo "stacks in the ${ENVIRONMENT} environment!"
echo "Region: ${REGION}"
echo "============================================="
read -rp "Type 'yes' to confirm: " CONFIRM
if [[ "${CONFIRM}" != "yes" ]]; then
    echo "Aborted."
    exit 0
fi

# Stacks in reverse dependency order
STACKS=(
    "04-athena-workgroup"
    "03-iam-roles"
    "02-glue-catalog"
    "01-s3-datalake"
)

empty_bucket() {
    local bucket_name="$1"
    echo ">>> Emptying bucket: ${bucket_name}"
    if ! aws s3api head-bucket --bucket "${bucket_name}" 2>/dev/null; then
        echo ">>> Bucket ${bucket_name} does not exist, skipping."
        return
    fi

    # Remove current objects (non-versioned fast path)
    aws s3 rm "s3://${bucket_name}" --recursive --region "${REGION}" || true

    # Paginate through ALL versions and delete in batches of 1000
    local key_marker="" version_marker="" truncated="true"
    while [[ "${truncated}" == "true" ]]; do
        local page_args=(--bucket "${bucket_name}" --output json --max-items 1000)
        if [[ -n "${key_marker}" ]]; then
            page_args+=(--key-marker "${key_marker}" --version-id-marker "${version_marker}")
        fi

        local page
        page=$(aws s3api list-object-versions "${page_args[@]}" 2>/dev/null) || break

        # Delete versions in this page
        local versions
        versions=$(echo "${page}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
objs = [{'Key': v['Key'], 'VersionId': v['VersionId']}
        for v in data.get('Versions', []) or []]
objs += [{'Key': m['Key'], 'VersionId': m['VersionId']}
         for m in data.get('DeleteMarkers', []) or []]
if objs:
    print(json.dumps({'Objects': objs, 'Quiet': True}))
else:
    print('')
")

        if [[ -n "${versions}" ]]; then
            aws s3api delete-objects \
                --bucket "${bucket_name}" \
                --delete "${versions}" \
                --region "${REGION}" > /dev/null 2>&1 || true
        fi

        truncated=$(echo "${page}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print('true' if data.get('IsTruncated', False) else 'false')
")

        if [[ "${truncated}" == "true" ]]; then
            key_marker=$(echo "${page}" | python3 -c "
import sys, json; data = json.load(sys.stdin)
print(data.get('NextKeyMarker', ''))
")
            version_marker=$(echo "${page}" | python3 -c "
import sys, json; data = json.load(sys.stdin)
print(data.get('NextVersionIdMarker', ''))
")
        fi
    done

    echo ">>> Bucket ${bucket_name} emptied."
}

# Empty S3 buckets before stack deletion
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo ""
echo ">>> Emptying S3 buckets..."
empty_bucket "${PROJECT}-datalake-${ENVIRONMENT}-${ACCOUNT_ID}"
empty_bucket "${PROJECT}-athena-results-${ENVIRONMENT}-${ACCOUNT_ID}"

delete_stack() {
    local template_name="$1"
    local stack_name="${PROJECT}-${template_name}-${ENVIRONMENT}"

    echo ""
    echo ">>> Deleting stack: ${stack_name}"
    if aws cloudformation describe-stacks --stack-name "${stack_name}" --region "${REGION}" >/dev/null 2>&1; then
        aws cloudformation delete-stack \
            --stack-name "${stack_name}" \
            --region "${REGION}"
        echo ">>> Waiting for ${stack_name} to be deleted..."
        aws cloudformation wait stack-delete-complete \
            --stack-name "${stack_name}" \
            --region "${REGION}"
        echo ">>> Stack ${stack_name} deleted."
    else
        echo ">>> Stack ${stack_name} does not exist, skipping."
    fi
}

for stack in "${STACKS[@]}"; do
    delete_stack "${stack}"
done

echo ""
echo "============================================="
echo "All stacks destroyed successfully!"
echo "============================================="
