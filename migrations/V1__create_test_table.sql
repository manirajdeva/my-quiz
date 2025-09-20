use database sample123;
use schema sample_sch;

-- V1__create_test_table.sql (safe test)
CREATE OR REPLACE TABLE IF NOT EXISTS TEST_TABLE_CI (
  ID INT,
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);
