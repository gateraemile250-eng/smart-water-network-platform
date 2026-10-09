# AWS Account Setup and Verification

This document records how the AWS account was secured and verified
before any data was published. It contains no account IDs, user IDs,
credentials or the real bucket name.

## 1. Account

| Item | Value |
|---|---|
| Sign-up type | New AWS sign-up experience (AWS Builder ID, one project) |
| Plan | Free plan |
| Credits | USD 100 at the start |
| Free period | Ends on 8 April 2027 |
| Region | Europe (Stockholm), `eu-north-1`, assigned by AWS |
| Payment method | None added |

**Never use the Upgrade option.** On the Free plan, access ends when the
credits are used or the free period ends, and nothing is billed.

## 2. Account security

| Control | Status |
|---|---|
| Authenticator-app MFA | Enabled |
| Recovery email | Added |
| Zero-spend budget alert | Created |

A recovery email helps with account recovery. It is not a second sign-in
method.

## 3. Programmatic access

| Item | Value |
|---|---|
| IAM user | `smart-water-cli` (programmatic access only, no console password) |
| Policy | `SmartWaterLakeAccess`, a customer-managed policy attached directly to the user |
| Policy definition | `cloud/iam/smart-water-lake-access-policy.json` |

The policy allows only:

| Scope | Actions |
|---|---|
| The bucket | `s3:ListBucket`, `s3:GetBucketLocation` |
| Objects in the bucket | `s3:GetObject`, `s3:PutObject` |

Nothing else is allowed. The user cannot delete objects, change bucket
policies or lifecycle rules, or use any other AWS service. Permissions for
Glue and Athena will be added in the phases that need them, each with its
own review.

**Known trade-off:** long-term access keys are less safe than temporary
credentials. The new sign-up experience does not allow IAM users with
console access, so this setup uses a narrowly scoped user, keeps the keys
outside the repository, and plans to delete the keys at the end of the
project.

## 4. Credential handling

| Rule | Verification |
|---|---|
| Credentials live only in the AWS CLI configuration in the user profile | A credentials file exists in the user profile and no `.aws` folder exists in the project |
| Credential and key patterns are ignored by Git | `git check-ignore` confirmed `.aws/`, `*.pem` and `*.key` |
| No credential-like files are tracked | `git ls-files` matched nothing for `.aws`, `.pem`, `.key` or `credentials` |
| No access key was ever committed | `git log --all -S"AKIA"` and `-S"aws_secret_access_key"` returned nothing |
| No AWS settings in the PostgreSQL `.env` file | That file is passed whole into the Spark container, so it must not hold AWS settings |
| Real bucket name and account ID stay out of the repository | Documents and templates use `<bucket-name>` |

## 5. Verification record

| Check | Command | Result |
|---|---|---|
| Identity | `aws sts get-caller-identity` | Returns the `smart-water-cli` user |
| Bucket region | `aws s3api get-bucket-location --bucket <bucket-name>` | `eu-north-1` |
| HTTPS access | `aws s3 ls s3://<bucket-name>/` | Succeeds (the bucket was empty) |
| Plain HTTP blocked | Same command with an `http://` endpoint | `AccessDenied` with an explicit deny in a resource-based policy |
| Write test | Uploaded a test object, listed it, then deleted it in the console | Worked as expected |

## 6. Bucket configuration

See [data-lake-layout.md](data-lake-layout.md) for the zone layout and
security settings, and `cloud/s3/` for the bucket policy and lifecycle
rules. These were applied in the AWS console. The JSON files record the
equivalent configuration and have not been applied through the CLI.

## 7. Planned configuration for publishing (Phase 4)

The publish scripts will read the bucket name from an environment
variable or from an ignored file, never from committed code. AWS settings
must not be added to `infrastructure/postgres/.env`.

## 8. Rebuilding the setup from scratch

1. Create an AWS account with the new sign-up experience on the Free plan,
   enable MFA and create a zero-spend budget alert.
2. Create the bucket in the assigned region with Block Public Access on,
   ACLs disabled and SSE-S3 encryption, and leave versioning off.
3. Apply the bucket policy and lifecycle rules from `cloud/s3/` after
   substituting the bucket name.
4. Create the IAM policy from `cloud/iam/` and a programmatic user with
   that policy attached.
5. Configure the AWS CLI and run the verification checks in section 5.
6. Clean up when finished: delete the access keys, empty and delete the
   bucket and delete the IAM user and policy.
