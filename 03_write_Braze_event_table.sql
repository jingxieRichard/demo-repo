/*-----------------------------------------------------------------------------
Snowflake Practice: Intro to Data Engineering with Snowpark Python
Script:       03_write_Braze_event_tables.sql
Author:       Jing Xie
Last Updated: 10/17/2024
-----------------------------------------------------------------------------*/

USE ROLE JING_DEMO_ROLE;
USE WAREHOUSE JING_DEMO_WH;
USE SCHEMA JING_DEMO_DB.RAW;

-- ----------------------------------------------------------------------------
-- Step 1: Create the stored procedure to load Braze raw event tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE LOAD_BRAZE_RAW_EVENT_TABLE()
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
    schema_name = "RAW"
    src_table_list = ["BRAZE_USER_EVENT_DEMO_DATASET.PUBLIC.USERS_MESSAGES_CONTENTCARD_SEND_VIEW",
                   "BRAZE_USER_EVENT_DEMO_DATASET.PUBLIC.USERS_MESSAGES_CONTENTCARD_IMPRESSION_VIEW",
                   "BRAZE_USER_EVENT_DEMO_DATASET.PUBLIC.USERS_MESSAGES_CONTENTCARD_CLICK_VIEW",
                   "BRAZE_USER_EVENT_DEMO_DATASET.PUBLIC.USERS_BEHAVIORS_PURCHASE_VIEW"]
    selected_columns = [["USER_ID", "TIME", "CONTENT_CARD_ID", "CAMPAIGN_ID"],
                    ["USER_ID", "TIME", "CONTENT_CARD_ID", "CAMPAIGN_ID"],
                    ["USER_ID", "TIME", "CONTENT_CARD_ID", "CAMPAIGN_ID"],
                    ["USER_ID", "TIME", "PRODUCT_ID", "PROPERTIES"]]
    dst_table_list = ["JING_DEMO_DB.RAW.USER_SENDS", "JING_DEMO_DB.RAW.USER_IMPRESSIONS",
                      "JING_DEMO_DB.RAW.USER_CLICKS","JING_DEMO_DB.RAW.USER_PURCHASES"]
                      

    ## Load the raw event tables
    for i in range(len(src_table_list)):
        src_table = session.table(src_table_list[i]).select(*selected_columns[i])
        if not table_exists(session, schema=schema_name, name=dst_table_list[i]):
           src_table.write.mode("overwrite").save_as_table(dst_table_list[i])
           print(f"Successfully created {dst_table_list[i]}")
        else:
           src_table.write.mode("append").save_as_table(dst_table_list[i])
           print(f"Successfully updated {dst_table_list[i]}")

    return f"All dst tables are created/updated successfully"

 $$;

 -- Step 2: Load the Braze raw events
 CALL LOAD_BRAZE_RAW_EVENT_TABLE();

 DESCRIBE TABLE USER_SENDS;
 SELECT * FROM USER_SENDS;

DESCRIBE TABLE USER_IMPRESSIONS;
SELECT * FROM USER_IMPRESSIONS;

DESCRIBE TABLE USER_CLICKS;
SELECT * FROM USER_CLICKS;

DESCRIBE TABLE USER_PURCHASES;
SELECT * FROM USER_PURCHASES;
 

 

 

    
