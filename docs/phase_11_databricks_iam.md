# Phase 11.6 — Databricks and AWS IAM Integration

## Objective

Establish and document the AWS IAM configuration supporting the
Databricks Unity Catalog integration for the Smart Water Network Platform.

Databricks is used as an additional lakehouse and analytics environment
alongside the existing AWS, Spark, Airflow, PostgreSQL, machine-learning,
and GIS components.

---

## 1. Databricks Storage Credential

Storage credential:

`smart_water_s3_credential`

Credential type:

`IAM Role`

Purpose:

`STORAGE`

Configured IAM role:

`arn:aws:iam::453914763430:role/SmartWaterDatabricksS3Role`

Configured external ID:

`1dc3340a-a670-4663-8d1c-34ae5fb8c858`

The Databricks credential is configured to use the dedicated
Smart Water AWS IAM role.

---

## 2. AWS IAM Role

Role:

`SmartWaterDatabricksS3Role`

AWS account:

`453914763430`

Purpose:

Provide controlled access to Smart Water project data stored in the
Amazon S3 data lake.

S3 bucket:

`smart-water-lake-453914763430`

---

## 3. IAM Trust Configuration

The AWS IAM trust relationship was inspected through the AWS Console.

The trust relationship includes the Databricks Unity Catalog production
role:

`arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL`

The role also contains the configured self-assume relationship.

The trust policy uses:

`sts:AssumeRole`

The external ID configured in the trust relationship matches the external
ID configured in the Databricks storage credential.

---

## 4. S3 Access Permissions

The Smart Water Databricks IAM role contains permissions required for
read access to the project S3 data lake.

Configured permissions include:

- `s3:ListBucket`
- `s3:GetBucketLocation`
- `s3:GetObject`

Bucket-level access:

`arn:aws:s3:::smart-water-lake-453914763430`

Object-level access:

`arn:aws:s3:::smart-water-lake-453914763430/*`

The role also contains the required self-assume permission:

`sts:AssumeRole`

---

## 5. Databricks Lakehouse Integration

The Databricks environment was configured as an additional lakehouse
processing and analytics layer.

The Phase 10 Databricks implementation demonstrated:

- Unity Catalog organization
- Unity Catalog Volume usage
- Delta Lake Bronze layer
- Delta Lake Silver layer
- Delta Lake Gold layer
- Data-quality validation
- Daily sensor analytics
- ML/anomaly-result integration
- Databricks SQL analytics

The Databricks environment therefore complements the existing Smart Water
data engineering architecture.

---

## 6. Independent AWS Validation

The AWS S3 data lake was independently validated using the AWS CLI.

The project S3 bucket was accessible and contained the expected processed
and curated Smart Water datasets.

Validated data included:

- Processed sensor readings
- Curated anomaly results
- Curated localization results
- Network nodes
- Network links
- Predictive/anomaly fusion results

This confirms that the AWS cloud data lake remains operational.

---

## 7. Integration Design

The intended architecture is:

    Smart Water Data
           |
           v
       AWS S3
           |
           v
    Databricks / Unity Catalog
           |
           v
       Delta Lake
      /    |     \
  Bronze  Silver  Gold
                  |
                  v
          Analytics / ML
                  |
                  v
              Dashboard

Databricks is an additional analytical and lakehouse layer and does not
replace the existing production components.

---

## 8. Relationship With the Core Platform

The core Smart Water platform remains based on:

- Apache Kafka
- Apache Spark
- Amazon S3
- PostgreSQL/PostGIS
- Apache Airflow
- Machine-learning pipelines
- Predictive analytics
- Network localization
- GIS analysis
- Streamlit dashboard

Databricks extends this architecture by providing a modern lakehouse
environment for scalable analytical processing and Delta-based data
management.

---

## 9. Final Assessment

The Databricks and AWS IAM integration configuration has been established
and documented.

The following configuration elements were inspected or confirmed through
the available AWS and Databricks interfaces:

- Databricks storage credential
- AWS IAM role
- IAM role ARN
- Unity Catalog trust relationship
- External ID configuration
- S3 bucket permissions
- AWS S3 data lake accessibility

The AWS S3 data lake was independently validated using the AWS CLI.

The end-to-end Databricks-to-AWS `AssumeRole` path remains a validation
boundary in this environment. The configured role, trust relationship and
permissions are documented, but a successful independent live
`AssumeRole` validation was not established with the available AWS CLI
identity.

This limitation does not affect the validated core Smart Water processing
and analytics pipeline. Databricks remains an additional lakehouse
integration rather than a dependency for the core platform.

---

## 10. Phase 11.6 Outcome

Phase 11.6 completed the configuration and documentation of the Databricks
and AWS IAM integration.

The project now has a documented lakehouse integration architecture
connecting Databricks, Unity Catalog, AWS IAM and the Smart Water S3 data
lake.

The integration boundary and its remaining validation limitation are
explicitly documented rather than presented as a successful end-to-end
IAM test.
