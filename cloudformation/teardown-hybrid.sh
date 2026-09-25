#!/usr/bin/env bash
# Tear down the optional hybrid-access and Databricks storage-role stacks (06, 05).
# Run this BEFORE cloudformation/teardown-all.sh empties/deletes the S3 stack (01),
# and only after removing dependent Unity Catalog external locations/credentials
# in Databricks (terraform destroy of the workspace/credential roots, or manual
# deletion in the workspace UI). This script does not touch Databricks itself.
# Usage: ./teardown-hybrid.sh [dev|prod]
set -euo pipefail

ENVIRONMENT="${1:-dev}"
PROJECT="stock-market-pipeline"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"

echo "============================================="
echo "WARNING: This will DESTROY the hybrid-access and"
echo "Databricks storage-role stacks (${ENVIRONMENT})."
echo "Region: ${REGION}"
echo "Ensure dependent Unity Catalog external locations/credentials"
echo "were already removed in Databricks."
echo "============================================="
read -rp "Type 'yes' to confirm: " CONFIRM
if [[ "${CONFIRM}" != "yes" ]]; then
    echo "Aborted."
    exit 0
fi

# Stacks in reverse dependency order (06 imports 05's policy ARN)
STACKS=(
    "06-databricks-storage-role"
    "05-hybrid-access"
)

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
echo "Hybrid stacks destroyed."
echo "Run cloudformation/teardown-all.sh next to remove stacks 01-04."
echo "============================================="
