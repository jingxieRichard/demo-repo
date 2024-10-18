/*-----------------------------------------------------------------------------
Snowflake Practice: Intro to Data Engineering with Snowpark Python
Script:       04_aggregate_Braze_user_events.sql
Author:       Jing Xie
Last Updated: 10/17/2024
-----------------------------------------------------------------------------*/

USE ROLE JING_DEMO_ROLE;
USE WAREHOUSE JING_DEMO_WH;
USE SCHEMA JING_DEMO_DB.RAW;

-- ----------------------------------------------------------------------------
-- Step 1: Create the stored procedure to load Braze raw event tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE AGGREGATE_BRAZE_USER_EVENTS_SP()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS 
$$
from snowflake.snowpark import Session 
import snowflake.snowpark.functions as F 

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def main(session: Session) -> str:
    schema_name = "TRANSFORM"
    table_name = "AGGREGATED_USER_EVENTS"

    raw_user_sends = session.table("JING_DEMO_DB.RAW.USER_SENDS")
    raw_user_impressions = session.table("JING_DEMO_DB.RAW.USER_IMPRESSIONS")
    raw_user_clicks = session.table("JING_DEMO_DB.RAW.USER_CLICKS")
    raw_user_purchases = session.table("JING_DEMO_DB.RAW.USER_PURCHASES")
    
     
    agg_user_sends = (raw_user_sends
                 .group_by("USER_ID").agg(F.count("*").alias("Number_sends")))
    agg_user_impressions = (raw_user_impressions
                 .group_by("USER_ID").agg(F.count("*").alias("Number_impressions")))
    agg_user_clicks = (raw_user_clicks
                 .group_by("USER_ID").agg(F.count("*").alias("Number_clicks")))
    agg_user_purchases = (raw_user_purchases
                 .group_by("USER_ID").agg(F.count("*").alias("Number_purchases")))

    agg_user_events = (agg_user_sends
                  .join(agg_user_impressions, "USER_ID", "outer")
                  .join(agg_user_clicks, "USER_ID", "outer")
                  .join(agg_user_purchases, "USER_ID", "outer"))
    if not table_exists(session, schema=schema_name, name=table_name):
        agg_user_events.write.mode("overwrite").save_as_table(f"{schema_name}.{table_name}")
        return f"Successfully created {table_name}"
    else:
           agg_user_events.write.mode("append").save_as_table(f"{schema_name}.{table_name}")
           return f"Successfully updated {table_name}"

$$;

-- Step 2: Load the aggregated user events
 CALL AGGREGATE_BRAZE_USER_EVENTS_SP();

 DESCRIBE TABLE TRANSFORM.AGGREGATED_USER_EVENTS;
 SELECT * FROM TRANSFORM.AGGREGATED_USER_EVENTS;
