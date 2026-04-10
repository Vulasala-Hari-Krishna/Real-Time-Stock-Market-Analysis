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

    # Delete ALL object versions and delete markers.
    # Loop: list up to 1000 → delete them → repeat until empty.
    local batch=0
    while true; do
        local page
        page=$(aws s3api list-object-versions \
            --bucket "${bucket_name}" \
            --max-keys 1000 \
            --region "${REGION}" \
            --output json 2>/dev/null) || break

        # Build delete payload from versions + delete markers
        local tmpfile
        tmpfile=$(mktemp)
        echo "${page}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
objs = [{'Key': v['Key'], 'VersionId': v['VersionId']}
        for v in data.get('Versions') or []]
objs += [{'Key': m['Key'], 'VersionId': m['VersionId']}
         for m in data.get('DeleteMarkers') or []]
if objs:
    print(json.dumps({'Objects': objs, 'Quiet': True}))
" > "${tmpfile}"

        # If nothing to delete, we're done
        if [[ ! -s "${tmpfile}" ]]; then
            rm -f "${tmpfile}"
            break
        fi

        batch=$((batch + 1))
        echo "    Deleting version batch ${batch}..."
        aws s3api delete-objects \
            --bucket "${bucket_name}" \
            --delete "file://${tmpfile}" \
            --region "${REGION}" > /dev/null 2>&1 || true
        rm -f "${tmpfile}"
    done

    echo ">>> Bucket ${bucket_name} emptied (${batch} version batches deleted)."
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
