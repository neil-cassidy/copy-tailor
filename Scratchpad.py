# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC %pip install databricks-genai
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.local_products
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS
# MAGIC SELECT product_id, product_description
# MAGIC FROM bright_data_myntra_products.datasets.myntra_products_1;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.product_embeddings 
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   ai_query('databricks-gte-large-en', product_description) as embeddings
# MAGIC FROM
# MAGIC   bright_data_myntra_products.datasets.myntra_products_1;

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient(disable_notice=True)

index = client.create_delta_sync_index(
    embedding_dimension=1024,
    embedding_model_endpoint_name="databricks-gte-large-en",
    embedding_vector_column="embeddings",
    endpoint_name="vector-search",
    index_name="workspace.default.product_embeddings_index_2",
    primary_key="product_id",
    pipeline_type="TRIGGERED",
    source_table_name="workspace.default.product_embeddings",
)

# COMMAND ----------

index.sync()

# COMMAND ----------

index.describe()

# COMMAND ----------

from databricks_genai_inference import Embedding

response = Embedding.create(
    model="databricks-gte-large-en",
    input="3D ActionSLAM: wearable person tracking in multi-floor environments")

results = index.similarity_search(
    query_vector=response,
    columns=["product_id"],
    num_results=5,
)
