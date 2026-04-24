-- ====================================================
-- LOAD DATA FROM S3 INTO REDSHIFT (COPY COMMANDS)
-- ====================================================

-- Replace IAM_ROLE with your Redshift IAM Role ARN

-- ==============================
-- CLEANED TABLES
-- ==============================

-- Load claims data
COPY project_cleaned.claims
FROM 's3://healthcare-insurance-data-platform/output-data/cleaned_claims/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS JSON 'auto';

-- Load patients data
COPY project_cleaned.patients
FROM 's3://healthcare-insurance-data-platform/output-data/cleaned_patients/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Load subscriber data
COPY project_cleaned.subscriber
FROM 's3://healthcare-insurance-data-platform/output-data/cleaned_subscriber/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Load group-subgroup mapping
COPY project_cleaned.group_subgroup
FROM 's3://healthcare-insurance-data-platform/output-data/cleaned_group_subgroup/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Load group data (from input)
COPY project_cleaned.groups
FROM 's3://healthcare-insurance-data-platform/input-data/group.csv'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Load hospital data
COPY project_cleaned.hospital
FROM 's3://healthcare-insurance-data-platform/input-data/hospital.csv'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Load disease data
COPY project_cleaned.disease
FROM 's3://healthcare-insurance-data-platform/input-data/disease.csv'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- ==============================
-- OUTPUT TABLES (USE CASES)
-- ==============================

-- Use Case 1
COPY project_output.disease_max_claims
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_01_disease_max_claims/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 2
COPY project_output.subscribers_age_lt_30_subgroup
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_02_subscribers_age_lt_30_subgroup/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 3
COPY project_output.group_max_subgroups
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_03_group_max_subgroups/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 4
COPY project_output.hospital_most_patients
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_04_hospital_most_patients/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 5
COPY project_output.subgroup_most_subscriptions
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_05_subgroup_most_subscriptions/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 6
COPY project_output.total_rejected_claims
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_06_total_rejected_claims/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 7
COPY project_output.city_most_claims
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_07_city_most_claims/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 8
COPY project_output.policy_type_most_subscribed
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_08_policy_type_most_subscribed/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 9
COPY project_output.avg_monthly_premium
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_09_avg_monthly_premium/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 10
COPY project_output.most_profitable_group
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_10_most_profitable_group/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;

-- Use Case 11
COPY project_output.patients_below_18_cancer
FROM 's3://healthcare-insurance-data-platform/output-data/usecase_11_patients_below_18_cancer/'
IAM_ROLE 'YOUR_IAM_ROLE'
FORMAT AS CSV
IGNOREHEADER 1;
