/*-----------------------------------------------------------------------------
Snowflake Practice: Intro to Data Engineering with Snowpark Python
Script:       02_load_Braze_events.sql
Author:       Jing Xie
Last Updated: 10/17/2024
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Data sharing/marketplace (instead of ETL)


USE ROLE JING_DEMO_ROLE;
USE WAREHOUSE JING_DEMO_WH;


-- ----------------------------------------------------------------------------
-- Step #1: Connect to Braze data in Marketplace
-- ----------------------------------------------------------------------------

/* 
    -> Snowsight Home Button
         -> Marketplace
             -> Search: "Braze User Event Demo Dataset" (and click on tile in results)
                 -> Click the blue "Get" button
                     -> Under "Options", adjust the Database name to read "Braze User Event Demo Dataset" (all capital letters)
                        -> Grant to "JING_DEMO_ROLE"
    
That's it... we don't have to do anything from here to keep this data updated.
The provider will do that for us and data sharing means we are always seeing
whatever they they have published.



GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE TO ROLE HOL_ROLE;
---*/


-- Let's look at the data - same 3-part naming convention as any other table
SELECT * FROM BRAZE_USER_EVENT_DEMO_DATASET.PUBLIC.USERS_MESSAGES_CONTENTCARD_CLICK_VIEW LIMIT 100;