-- This script sets up the initial Snowflake infrastructure including databases,
-- schemas, file formats, stages, and data security policies

--- Switch to sysadmin role for administrative operations
use role sysadmin;


-- Create and set up database and schema hierarchy
-- Development sandbox with different layers for data processing
create database if not exists sandbox;
use database sandbox;


-- Create schemas for different data processing stages:
-- stage_sch: Raw data landing
-- clean_sch: Cleaned/transformed data
-- consumption_sch: Business-ready data
-- common: Shared objects and configurations
create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;


-- Set active schema for staging operations
use schema stage_sch;


-- Create file format for CSV ingestion
-- Configures CSV processing with specific delimiters and headers
create file format if not exists stage_sch.csv_file_format
    type = 'csv'
    compression = 'auto'
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1
    field_optionally_enclosed_by = '\042'
    null_if = ('\\N');


--- Drop the file format
DROP FILE FORMAT IF EXISTS stage_sch.csv_file_format;


-- Create internal stage for file loading
create stage stage_sch.csv_stg
    directory = ( enable = true)
    comment = 'this is snowflake internal stage';


-- Data Security Configuration
-- Create PII policy tag with allowed classification values
create or replace tag
    common.pii_policy_tag
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'This is PII policy tag object';


-- Create masking policies for different types of sensitive data
-- PII masking policy
create or replace masking policy
    common.pii_masking_policy as (pii_text string)
    returns string ->
    to_varchar('** PII **');


-- Email masking policy
create or replace masking policy
    common.email_masking_policy as (email_text string)
    returns string ->
    to_varchar('** EMAIL **');

-- Phone number masking policy
create or replace masking policy
    common.phone_masking_policy as (phone string)
    returns string ->
    to_varchar('** Phone **');
