# Databricks notebook source
# MAGIC %pip install tabula-py jpype1 databricks-vectorsearch==0.22 databricks-sdk==0.12.0 mlflow[databricks]==2.9.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import numpy as np
import pandas as pd
import time
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c
from mlflow import MlflowClient
from typing import List
from functools import reduce
import pyspark
import re
import mlflow

def get_latest_model_version(model_name: str) -> int:
    mlflow_client = MlflowClient()
    latest_version = 1
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
    for i in range(180):
        endpoint = vsc.get_endpoint(vs_endpoint_name)
        status = endpoint.get("endpoint_status", endpoint.get("status"))[
            "state"
        ].upper()
        if "ONLINE" in status:
            return endpoint
        elif "PROVISIONING" in status or i < 6:
            if i % 20 == 0:
                print(
                    f"Waiting for endpoint to be ready, this can take a few min... {endpoint}"
                )
            time.sleep(10)
        else:
            print(endpoint)
            raise Exception(
                f"""Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")"""
            )
    raise Exception(
        f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}"
    )


def index_exists(vsc, endpoint_name: str, index_full_name: str):
    try:
        dict_vsindex = vsc.get_index(endpoint_name, index_full_name).describe()
        return dict_vsindex.get("status").get("ready", False)
    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" not in str(e):
            print(
                f"Unexpected error describing the index. This could be a permission issue."
            )
            raise e
    return False


def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
    for i in range(180):
        idx = vsc.get_index(vs_endpoint_name, index_name).describe()
        index_status = idx.get("status", idx.get("index_status", {}))
        status = index_status.get(
            "detailed_state", index_status.get("status", "UNKNOWN")
        ).upper()
        url = index_status.get("index_url", index_status.get("url", "UNKNOWN"))
        if "ONLINE" in status:
            return
        if "UNKNOWN" in status:
            print(
                f"Can't get the status - will assume index is ready {idx} - url: {url}"
            )
            return
        elif "PROVISIONING" in status:
            if i % 40 == 0:
                print(
                    f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}"
                )
            time.sleep(10)
        else:
            raise Exception(
                f"""Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}"""
            )
    raise Exception(
        f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}"
    )

# COMMAND ----------

import time
import pandas as pd
import databricks.sdk.service.catalog as c
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient

# COMMAND ----------

VECTOR_SEARCH_ENDPOINT_NAME = "vector-search1"
DEV_CATALOG = "workspace"
VECTOR_INDEX_SCHEMA = "default"
SOURCE_DATA_FOR_INDEX_TABLE_NAME = "product_details_2"
EMBEDDING_ENDPOINT_NAME = "databricks-bge-large-en"
SOURCE_TABLE_FULL_NAME = f"{DEV_CATALOG}.{VECTOR_INDEX_SCHEMA}.{SOURCE_DATA_FOR_INDEX_TABLE_NAME}"
SOURCE_TABLE_WITH_ID_FULL_NAME = f"{SOURCE_TABLE_FULL_NAME}_for_index"
VS_INDEX_FULL_NAME = f"{SOURCE_TABLE_FULL_NAME}_vs"



# COMMAND ----------

vsc = VectorSearchClient()

if VECTOR_SEARCH_ENDPOINT_NAME not in [
    e["name"] for e in vsc.list_endpoints().get("endpoints", [])
]:
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

df = spark.sql("select * from bright_data_myntra_products.datasets.myntra_products_1")

# COMMAND ----------

# List of column names
column_names = [
    "product_id bigint",
    "title string",
    "product_description string",
    "rating double",
    "ratings_count bigint",
    "final_price string",
    "currency string",
    "images string",
    "product_details string",
    "product_specifications string",
    "amount_of_stars string",
    "what_customers_said string",
    "seller_name string",
    "sizes string",
    "combined_column string"
]

# Extract column names from the list
column_names_list = [col.split()[0] for col in column_names]

print(column_names_list)


# COMMAND ----------

print(SOURCE_TABLE_WITH_ID_FULL_NAME)

# COMMAND ----------

schema_changed = True

if schema_changed:
    spark.sql(f"DROP TABLE IF EXISTS {SOURCE_TABLE_WITH_ID_FULL_NAME};")

spark.sql(
    f"""
        CREATE TABLE {"" if schema_changed else "IF NOT EXISTS"} {SOURCE_TABLE_WITH_ID_FULL_NAME} (
            product_id bigint,
            title string,
            product_description string,
            rating double,
            ratings_count bigint,
            final_price string,
            currency string,
            images string,
            product_details string,
            product_specifications string,
            amount_of_stars string,
            what_customers_said string,
            seller_name string,
            sizes string,
            combined_column string
        ) TBLPROPERTIES (delta.enableChangeDataFeed = true);
    """
)


(
    spark.table(SOURCE_TABLE_FULL_NAME)
        .drop('last_updated_dt_utc')
        .dropna()
        .write
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .option("mergeSchema", "true")  # Add this line
            .format("delta")
            .saveAsTable(SOURCE_TABLE_WITH_ID_FULL_NAME)
)

# COMMAND ----------

print(SOURCE_TABLE_WITH_ID_FULL_NAME)

# COMMAND ----------

# MAGIC %sql select * from workspace.default.product_details_2
# MAGIC

# COMMAND ----------

vsc.delete_index("workspace.default.product_details_2_vs, vector-search1") 

# COMMAND ----------

vsc.delete_index(VECTOR_SEARCH_ENDPOINT_NAME, VS_INDEX_FULL_NAME)

# COMMAND ----------

# DBTITLE 1,create the sync for vector search and use `embedding_source_column="ddl_string"`
if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, VS_INDEX_FULL_NAME) or schema_changed:
    if schema_changed and index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, VS_INDEX_FULL_NAME):
        print(f"Deleted index {VS_INDEX_FULL_NAME} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}")
        vsc.delete_index(VECTOR_SEARCH_ENDPOINT_NAME, VS_INDEX_FULL_NAME)
    print(
        f"Creating index {VS_INDEX_FULL_NAME} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}..."
    )
    vsc.create_delta_sync_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
        index_name=VS_INDEX_FULL_NAME,
        source_table_name=f"{SOURCE_TABLE_WITH_ID_FULL_NAME}",
        pipeline_type="TRIGGERED",
        primary_key="product_id",
        embedding_source_column="combined_column",  # The column containing our text
        embedding_model_endpoint_name=EMBEDDING_ENDPOINT_NAME,  # The embedding endpoint used to create the embeddings
    )
else:
    # Trigger a sync to update our vs content with the new data saved in the table
    vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, VS_INDEX_FULL_NAME).sync()

# Let's wait for the index to be ready and all our embeddings to be created and indexed
wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, VS_INDEX_FULL_NAME)
print(f"index {VS_INDEX_FULL_NAME} on table {SOURCE_TABLE_WITH_ID_FULL_NAME} is ready")

# COMMAND ----------


