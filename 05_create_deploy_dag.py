from datetime import timedelta
from snowflake.snowpark import Session 
from snowflake.core import Root 
from snowflake.core.task import Task 
from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask 
from snowflake.core._common import CreateMode

def main(session: Session) -> str:
    database_name = "JING_DEMO_DB"
    schema_name = "RAW"
    warehouse_name = "JING_DEMO_WH"

    api_root = Root(session)

    # Define the DAG
    dag_name = "JING_DEMO_DAG"
    dag = DAG(dag_name, schedule=timedelta(days=1), use_func_return_value=True, warehouse=warehouse_name)
    with dag: 
        dag_task1 = DAGTask("LOAD_BRAZE_RAW_EVENT_TABLES", 
                            definition="CALL LOAD_BRAZE_RAW_EVENT_TABLE() ", warehouse=warehouse_name)
        dag_task2 = DAGTask("AGGREGATE_USE_EVENTS", 
                            definition="CALL AGGREGATE_BRAZE_USER_EVENTS_SP()", warehouse=warehouse_name)
        
        dag_task2 >> dag_task1
    
    # Create the DAG in Snowflake 
    schema = api_root.databases[database_name].schemas[schema_name]
    dag_op = DAGOperation(schema)

    dag_op.deploy(dag, mode=CreateMode.or_replace)

    # Confirm the creation of the task graph
    taskiter = dag_op.iter_dags(like='JING_DEMO_DAG')
    for dag_name in taskiter:
        print(dag_name)

    dag_op.run(dag)

    return f"Successfully created and started the DAG!"


# For local debugging 
if __name__ == '__main__':
    import os, sys
    # Add the utils package to our path and import the snowpark_utils function
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close() 