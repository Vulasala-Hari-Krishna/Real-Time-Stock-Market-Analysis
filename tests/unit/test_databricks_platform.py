"""Static trust-boundary checks; no AWS or Databricks API calls."""

from pathlib import Path

import pytest
from cfnlint.decode import decode


@pytest.fixture()
def template() -> dict:
    """Parse CloudFormation intrinsic functions using the existing lint library."""
    root = Path(__file__).resolve().parents[2]
    parsed, errors = decode(
        str(root / "cloudformation/06-databricks-storage-role.yaml")
    )
    assert not errors
    return parsed


def test_role_bootstraps_with_no_assumable_principal(template: dict) -> None:
    assert template["Parameters"]["EnableUnityCatalogTrust"]["Default"] == "false"
    role = template["Resources"]["DatabricksStorageRole"]["Properties"]
    condition, enabled, disabled = role["AssumeRolePolicyDocument"]["Fn::If"]
    assert condition == "TrustEnabled"
    assert disabled["Statement"] == [
        {
            "Sid": "BootstrapDenyAllAssumption",
            "Effect": "Deny",
            "Principal": {"AWS": "*"},
            "Action": "sts:AssumeRole",
        }
    ]
    assert len(enabled["Statement"]) == 2


def test_activation_requires_both_generated_values(template: dict) -> None:
    rule = template["Rules"]["RequireGeneratedTrustValues"]
    assert rule["RuleCondition"] == {
        "Fn::Equals": [{"Ref": "EnableUnityCatalogTrust"}, "true"]
    }
    asserted = {
        check["Assert"]["Fn::Not"][0]["Fn::Equals"][0]["Ref"]
        for check in rule["Assertions"]
    }
    assert asserted == {"UnityCatalogPrincipalArn", "UnityCatalogExternalId"}
    assert all(
        check["Assert"]["Fn::Not"][0]["Fn::Equals"][1] == ""
        for check in rule["Assertions"]
    )


def test_activated_trust_uses_exact_principals_and_external_id(template: dict) -> None:
    role = template["Resources"]["DatabricksStorageRole"]["Properties"]
    external, self_assume = role["AssumeRolePolicyDocument"]["Fn::If"][1]["Statement"]
    assert external["Principal"] == {"AWS": {"Ref": "UnityCatalogPrincipalArn"}}
    assert self_assume["Principal"] == {
        "AWS": {"Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:root"}
    }
    expected_self = {
        "Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:role/${ProjectName}-uc-storage-${Environment}"
    }
    assert self_assume["Condition"]["ArnEquals"]["aws:PrincipalArn"] == expected_self
    for statement in (external, self_assume):
        assert statement["Effect"] == "Allow"
        assert statement["Action"] == "sts:AssumeRole"
        assert statement["Condition"]["StringEquals"]["sts:ExternalId"] == {
            "Ref": "UnityCatalogExternalId"
        }
    assert role["Policies"][0]["PolicyDocument"]["Statement"] == [
        {"Effect": "Allow", "Action": "sts:AssumeRole", "Resource": expected_self}
    ]


def test_role_attaches_only_prepared_storage_policy(template: dict) -> None:
    role = template["Resources"]["DatabricksStorageRole"]["Properties"]
    assert role["ManagedPolicyArns"] == [
        {
            "Fn::ImportValue": {
                "Fn::Sub": "${ProjectName}-${Environment}-DatabricksStoragePolicyArn"
            }
        }
    ]
    assert role["MaxSessionDuration"] == 3600
    assert {tag["Key"] for tag in role["Tags"]} == {"Project", "Environment"}


def test_no_credentials_compute_or_instance_profiles(template: dict) -> None:
    assert [resource["Type"] for resource in template["Resources"].values()] == [
        "AWS::IAM::Role"
    ]
    assert set(template["Outputs"]) == {
        "DatabricksStorageRoleArn",
        "UnityCatalogTrustEnabled",
    }
