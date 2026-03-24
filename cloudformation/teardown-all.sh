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
    if aws s3api head-bucket --bucket "${bucket_name}" 2>/dev/null; then
        aws s3 rm "s3://${bucket_name}" --recursive --region "${REGION}"
        # Delete versioned objects if versioning is enabled
        local versions
        versions=$(aws s3api list-object-versions \
            --bucket "${bucket_name}" \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
            --output json 2>/dev/null)
        if [[ "${versions}" != "null" && "${versions}" != '{"Objects": null}' ]]; then
            echo "${versions}" | aws s3api delete-objects \
                --bucket "${bucket_name}" \
                --delete "$(echo "${versions}")" 2>/dev/null || true
        fi
        # Delete markers
        local markers
        markers=$(aws s3api list-object-versions \
            --bucket "${bucket_name}" \
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
            --output json 2>/dev/null)
        if [[ "${markers}" != "null" && "${markers}" != '{"Objects": null}' ]]; then
            echo "${markers}" | aws s3api delete-objects \
                --bucket "${bucket_name}" \
                --delete "$(echo "${markers}")" 2>/dev/null || true
        fi
        echo ">>> Bucket ${bucket_name} emptied."
    else
        echo ">>> Bucket ${bucket_name} does not exist, skipping."
    fi
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
