# Cloud configuration templates

These files record the AWS configuration of the optional cloud layer.
They contain the placeholder `<bucket-name>` instead of the real bucket
name, because the real name is account-specific and must not be committed.

| File | What it defines |
|---|---|
| `iam/smart-water-lake-access-policy.json` | Customer-managed IAM policy `SmartWaterLakeAccess` (list, locate, read and write objects in one bucket) |
| `s3/bucket-policy-tls-only.json` | Bucket policy that denies every request not made over HTTPS |
| `s3/lifecycle.json` | Two lifecycle rules: expire `athena-results/` after 7 days, and abort incomplete multipart uploads after 7 days |

## How the live configuration was applied

All three items were created **in the AWS console**. These files record
the equivalent configuration. Applying them with the AWS CLI has not been
tested.

The CLI user (`smart-water-cli`) deliberately cannot change bucket
policies or lifecycle rules. Those changes need the account owner's
identity.

## Using a template

1. Copy the file to a temporary location outside the repository.
2. Replace `<bucket-name>` with the real bucket name.
3. Never commit the edited copy.

PowerShell example (the lifecycle file has no placeholder and can be used
as it is):

```powershell
(Get-Content cloud\s3\bucket-policy-tls-only.json) -replace '<bucket-name>', 'your-bucket-name' |
    Set-Content "$env:TEMP\bucket-policy-tls-only.json"
```
