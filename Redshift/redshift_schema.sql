-- ====================================================
-- Healthcare Insurance Data Platform - Redshift Schema
-- ====================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS project_cleaned;
CREATE SCHEMA IF NOT EXISTS project_output;

-- ==============================
-- CLEANED TABLES
-- ==============================

-- Claims data
CREATE TABLE project_cleaned.claims (
claim_id BIGINT,
patient_id BIGINT,
sub_id VARCHAR(50),
claim_amount DECIMAL(18,2),
claim_date DATE,
claim_status VARCHAR(50),
claim_type VARCHAR(100),
disease_name VARCHAR(255)
);

-- Patient data
CREATE TABLE project_cleaned.patients (
patient_id BIGINT,
patient_name VARCHAR(255),
patient_gender VARCHAR(20),
patient_birth_date DATE,
patient_phone VARCHAR(50),
disease_name VARCHAR(255),
city VARCHAR(100),
hospital_id VARCHAR(50)
);

-- Subscriber data
CREATE TABLE project_cleaned.subscriber (
sub_id VARCHAR(50),
first_name VARCHAR(100),
last_name VARCHAR(100),
street VARCHAR(255),
birth_date DATE,
gender VARCHAR(20),
phone VARCHAR(50),
country VARCHAR(100),
city VARCHAR(100),
zip_code VARCHAR(20),
subgrp_id VARCHAR(50),
elig_ind VARCHAR(10),
eff_date DATE,
term_date DATE
);

-- Group - Subgroup mapping
CREATE TABLE project_cleaned.group_subgroup (
subgrp_id VARCHAR(50),
grp_id VARCHAR(50)
);

-- Group information
CREATE TABLE project_cleaned.groups (
country VARCHAR(100),
premium_written DECIMAL(18,2),
zipcode VARCHAR(20),
grp_id VARCHAR(50),
grp_name VARCHAR(255),
grp_type VARCHAR(50),
city VARCHAR(100),
year INT
);

-- Hospital data
CREATE TABLE project_cleaned.hospital (
hospital_id VARCHAR(50),
hospital_name VARCHAR(255),
city VARCHAR(100),
state VARCHAR(100),
country VARCHAR(100)
);

-- Disease mapping
CREATE TABLE project_cleaned.disease (
subgrp_id VARCHAR(50),
disease_id BIGINT,
disease_name VARCHAR(255)
);

-- ==============================
-- OUTPUT TABLES (USE CASES)
-- ==============================

-- Use Case 1
CREATE TABLE project_output.disease_max_claims (
disease_name VARCHAR(255),
total_claims INT
);

-- Use Case 2 (dates stored as text)
CREATE TABLE project_output.subscribers_age_lt_30_subgroup (
sub_id VARCHAR(50),
first_name VARCHAR(100),
last_name VARCHAR(100),
street VARCHAR(255),
birth_date VARCHAR(50),
gender VARCHAR(20),
phone VARCHAR(50),
country VARCHAR(100),
city VARCHAR(100),
zip_code VARCHAR(20),
subgrp_id VARCHAR(50),
elig_ind VARCHAR(10),
eff_date VARCHAR(50),
term_date VARCHAR(50),
age INT
);

-- Use Case 3
CREATE TABLE project_output.group_max_subgroups (
grp_id VARCHAR(50),
total_subgroups INT
);

-- Use Case 4
CREATE TABLE project_output.hospital_most_patients (
hospital_id VARCHAR(50),
total_patients INT
);

-- Use Case 5
CREATE TABLE project_output.subgroup_most_subscriptions (
subgrp_id VARCHAR(50),
total_subscriptions INT
);

-- Use Case 6
CREATE TABLE project_output.total_rejected_claims (
total_rejected_claims INT
);

-- Use Case 7
CREATE TABLE project_output.city_most_claims (
city VARCHAR(100),
total_claims INT
);

-- Use Case 8
CREATE TABLE project_output.policy_type_most_subscribed (
grp_type VARCHAR(50),
total_subscriptions INT
);

-- Use Case 9
CREATE TABLE project_output.avg_monthly_premium (
average_monthly_premium DECIMAL(18,2)
);

-- Use Case 10
CREATE TABLE project_output.most_profitable_group (
grp_id VARCHAR(50),
grp_name VARCHAR(255),
grp_type VARCHAR(50),
premium_written DECIMAL(18,2)
);

-- Use Case 11
CREATE TABLE project_output.patients_below_18_cancer (
patient_id BIGINT,
patient_name VARCHAR(255),
patient_gender VARCHAR(20),
patient_birth_date DATE,
disease_name VARCHAR(255),
city VARCHAR(100),
hospital_id VARCHAR(50),
age INT
);
