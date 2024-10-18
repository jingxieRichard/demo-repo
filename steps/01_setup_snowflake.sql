/*-----------------------------------------------------------------------------
Snowflake Learning Practice: Intro to Data Engineering with Snowpark Python
Script:       01_setup_snowflake.sql
Author:       Jing Xie
Last Updated: 10/17/2024
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Visual Studio Code Snowflake native extension (Git integration)


-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects (ACCOUNTADMIN part)
-- ----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

-- Roles 
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE JING_DEMO_ROLE;
GRANT ROLE JING_DEMO_ROLE TO ROLE SYSADMIN;
GRANT ROLE JING_DEMO_ROLE TO USER IDENTIFIER ($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE JING_DEMO_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE JING_DEMO_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO  JING_DEMO_ROLE;

-- Databases 
CREATE OR REPLACE DATABASE JING_DEMO_DB;
GRANT OWNERSHIP ON DATABASE JING_DEMO_DB TO ROLE JING_DEMO_ROLE;

-- WAREHOUSE 
CREATE OR REPLACE WAREHOUSE JING_DEMO_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME = TRUE;
GRANT OWNERSHIP ON WAREHOUSE JING_DEMO_WH TO ROLE JING_DEMO_ROLE;

-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE JING_DEMO_ROLE;
USE WAREHOUSE JING_DEMO_WH;
USE DATABASE JING_DEMO_DB;

-- Schemas 
CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA TRANSFORM;

