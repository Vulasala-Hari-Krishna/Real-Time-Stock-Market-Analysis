#!/usr/bin/env bash
# Deploy the optional hybrid-access and Databricks storage-role stacks (05, 06).
# These are intentionally excluded from deploy-all.sh; run this only when
# explicitly starting the Databricks hybrid slice, after stack 01 (S3 datalake)
# already exists in the same region.
#
# Usage:
#   ./deploy-hybrid.sh [dev|prod]
#
# First run (bootstrap, deny-all trust on stack 06):
#   ./deploy-hybrid.sh dev
#
# Second run, after the databricks/terraform/credential root has been applied
# with validate_storage_access=false and returned its unity_catalog_principal_arn
# / unity_catalog_external_id outputs:
#   ENABLE_UC_TRUST=true \
#   UC_PRINCIPAL_ARN="arn:aws:iam::<account-id>:role/<...>" \
#   UC_EXTERNAL_ID="<external-id>" \
#   ./deploy-hybrid.sh dev
set -euo pipefail

ENVIRONMENT="${1:-dev}"
PROJECT="stock-market-pipeline"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARAMS_FILE="${SCRIPT_DIR}/parameters/${ENVIRONMENT}.json"

ENABLE_UC_TRUST="${ENABLE_UC_TRUST:-false}"
UC_PRINCIPAL_ARN="${UC_PRINCIPAL_ARN:-}"
UC_EXTERNAL_ID="${UC_EXTERNAL_ID:-}"

if [[ ! -f "${PARAMS_FILE}" ]]; then
    echo "ERROR: Parameter file not found: ${PARAMS_FILE}"
    exit 1
fi

if [[ "${ENABLE_UC_TRUST}" == "true" && ( -z "${UC_PRINCIPAL_ARN}" || -z "${UC_EXTERNAL_ID}" ) ]]; then
    echo "ERROR: ENABLE_UC_TRUST=true requires UC_PRINCIPAL_ARN and UC_EXTERNAL_ID."
    echo "Get these from 'terraform output' in databricks/terraform/credential after"
    echo "its bootstrap apply (validate_storage_access=false, trust still disabled)."
    exit 1
fi

echo "============================================="
echo "Deploying ${PROJECT} hybrid stacks (${ENVIRONMENT})"
echo "Region: ${REGION}"
echo "Unity Catalog trust enabled: ${ENABLE_UC_TRUST}"
echo "============================================="

BASE_PARAMS=$(jq -r '.[] | "\(.ParameterKey)=\(.ParameterValue)"' "${PARAMS_FILE}" | tr '\n' ' ')

deploy_stack() {
    local template_name="$1"
    shift
    local extra_params=("$@")
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
        --parameter-overrides ${BASE_PARAMS} "${extra_params[@]}" \
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

deploy_stack "05-hybrid-access"
deploy_stack "06-databricks-storage-role" \
    "EnableUnityCatalogTrust=${ENABLE_UC_TRUST}" \
    "UnityCatalogPrincipalArn=${UC_PRINCIPAL_ARN}" \
    "UnityCatalogExternalId=${UC_EXTERNAL_ID}"

echo ""
echo "============================================="
echo "Hybrid stacks deployed. Unity Catalog trust enabled: ${ENABLE_UC_TRUST}"
echo "============================================="
