# Databricks notebook source

import prefect
from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.client.secrets import Secret

from prefect.tasks.databricks.databricks_submitjob import DatabricksSubmitRun
# from prefect.tasks.secrets import PrefectSecret




# databricks configuration
runtime_name=prefect.config.databricks.run_name
cluster_id=prefect.config.databricks.cluster_id
dir_path=prefect.config.databricks.dir_path



# get databricks_connection credental

conn = Secret("DATABRICKS_CONNECTION_STRING").get()

#get api credential
api_host = Secret("RAPID_API_HOST").get()
api_key = Secret("RAPID_API_KEY").get()
url=prefect.config.api.url








# submit configuration

def get_submit_config(NoteBookName,Notebook_params):
    task_setup = {
        "run_name": runtime_name,
        "existing_cluster_id": cluster_id,
        "notebook_task":{
            "notebook_path":f"/Repos/agoyal@sigmoidanalytics.com/Prefect_Databricks_Assignment/Tasks/{NoteBookName}",
            "base_parameters": Notebook_params,

        }

    }
    return task_setup


api_host = Secret("RAPID_API_HOST").get()
api_key = Secret("RAPID_API_KEY").get()
rapid_api_params = {"rapid_api_host": api_host, "rapid_api_key": api_key,"dir_name":dir_path,"url_name": url }




# Create flow

with Flow(name="Prefect-DataBricks-Ashutosh",schedule=None) as flow:
    #Data Extract
    submit_Task_1 = DatabricksSubmitRun(name="extract",databricks_conn_secret=conn,json=get_submit_config("extract",rapid_api_params))
    #Data Transform
    submit_Task_2 = DatabricksSubmitRun(name="Transform-Affected_Country",databricks_conn_secret=conn,json=get_submit_config("transform_most_affected_country",rapid_api_params))
    submit_Task_3 = DatabricksSubmitRun(name="Transform-Maximum_Case_Country", databricks_conn_secret=conn,json=get_submit_config("transform_most_cases",rapid_api_params))
    submit_Task_4 = DatabricksSubmitRun(name="Transform-Efficient_Case_Country",databricks_conn_secret=conn,json=get_submit_config("transform_most_recovered",rapid_api_params))
    #Data Load
    submit_Task_5 = DatabricksSubmitRun(name="Load", databricks_conn_secret=conn,json=get_submit_config("load",rapid_api_params))

    # Creating flow and dependencies between the tasks
    flow.add_edge(conn, submit_Task_1)
    flow.add_edge(submit_Task_1, submit_Task_2)
    flow.add_edge(submit_Task_1, submit_Task_3)
    flow.add_edge(submit_Task_1, submit_Task_4)
    flow.add_edge(submit_Task_2, submit_Task_5)
    flow.add_edge(submit_Task_3, submit_Task_5)
    flow.add_edge(submit_Task_4, submit_Task_5)



# Selecting Executor for parallel executions
flow.executor = LocalDaskExecutor()
flow.register("prefect-dataBricks-Ashutosh")


