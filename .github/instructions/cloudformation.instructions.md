---
applyTo: "cloudformation/**/*.yaml"
---

# CloudFormation Template Instructions

- Use `AWSTemplateFormatVersion: '2010-09-09'` in every template
- Every template must have a `Description` field
- Parameterize `Environment` (dev/prod) and `ProjectName` in every stack
- Use `!Sub` for string interpolation, `!Ref` for parameter references
- Use `Fn::ImportValue` for cross-stack references (outputs from other stacks)
- Every resource must have `Tags` with at minimum `Project` and `Environment`
- S3 buckets must block all public access
- IAM policies must follow least privilege — scope to specific resources, not `*` where possible
- Use `Export` on `Outputs` so other stacks can reference them
- Include `DeletionPolicy: Retain` on any resource holding production data (not needed for dev)
- Name stacks consistently: `{project}-{template-name}-{environment}`