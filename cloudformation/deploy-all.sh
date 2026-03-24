#!/usr/bin/env bash
# Deploy all CloudFormation stacks in dependency order.
# Usage: ./deploy-all.sh [dev|prod]
set -euo pipefail

ENVIRONMENT="${1:-dev}"
PROJECT="stock-market-pipeline"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARAMS_FILE="${SCRIPT_DIR}/parameters/${ENVIRONMENT}.json"

if [[ ! -f "${PARAMS_FILE}" ]]; then
    echo "ERROR: Parameter file not found: ${PARAMS_FILE}"
    exit 1
fi

echo "============================================="
echo "Deploying ${PROJECT} stacks (${ENVIRONMENT})"
echo "Region: ${REGION}"
echo "============================================="

STACKS=(
    "01-s3-datalake"
    "02-glue-catalog"
    "03-iam-roles"
    "04-athena-workgroup"
)

deploy_stack() {
    local template_name="$1"
    local stack_name="${PROJECT}-${template_name}-${ENVIRONMENT}"
    local template_file="${SCRIPT_DIR}/${template_name}.yaml"

    if [[ ! -f "${template_file}" ]]; then
        echo "ERROR: Template not found: ${template_file}"
        return 1
    fi

    echo ""
    echo ">>> Deploying stack: ${stack_name}"
    aws cloudformation deploy \
        --stack-name "${stack_name}" \
        --template-file "${template_file}" \
        --parameter-overrides "$(jq -r '.[] | "\(.ParameterKey)=\(.ParameterValue)"' "${PARAMS_FILE}" | tr '\n' ' ')" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "${REGION}" \
        --no-fail-on-empty-changeset

    echo ">>> Stack ${stack_name} deployed successfully."
    echo "--- Outputs ---"
    aws cloudformation describe-stacks \
        --stack-name "${stack_name}" \
        --region "${REGION}" \
        --query 'Stacks[0].Outputs' \
        --output table 2>/dev/null || echo "(no outputs)"
}

for stack in "${STACKS[@]}"; do
    deploy_stack "${stack}"
done

echo ""
echo "============================================="
echo "All stacks deployed successfully!"
echo "============================================="
