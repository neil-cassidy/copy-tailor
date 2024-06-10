# Databricks notebook source
# MAGIC
# MAGIC %pip install databricks-vectorsearch
# MAGIC %pip install databricks-genai
# MAGIC %pip install databricks-genai-inference
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import pandas as pd

# COMMAND ----------

vsc = VectorSearchClient()
index = vsc.get_index(endpoint_name="vector-search1", index_name="workspace.default.product_details_2_vs")
index.describe()


# COMMAND ----------


import time

while not index.describe().get('status').get('detailed_state').startswith('ONLINE'):

    print("Waiting for index to be ONLINE...")
    time.sleep(5)
print("Index is ONLINE")
index.describe()

# COMMAND ----------


all_column_names=['product_id', 'title', 'product_description', 'rating', 'ratings_count', 'final_price', 'currency', 'images', 'product_details', 'product_specifications', 'amount_of_stars', 'what_customers_said', 'seller_name', 'sizes', 'combined_column']
results = index.similarity_search(
  query_text="a white dress",
  columns=['product_id', 'title', 'product_description', 'rating', 'ratings_count', 'final_price', 'currency', 'images', 'product_details', 'product_specifications', 'amount_of_stars', 'what_customers_said', 'seller_name', 'sizes', 'combined_column'],
  num_results=5)

rows = results['result']['data_array']
df = pd.DataFrame(rows, columns=all_column_names + ['score'])
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC black color womens jacket
# MAGIC green color womens shorts
# MAGIC
# MAGIC

# COMMAND ----------

from databricks_genai_inference import ChatCompletion

# Only required when running this example outside of a Databricks Notebook

DATABRICKS_HOST="https://dbc-c91a7871-73b3.cloud.databricks.com/serving-endpoints"
DATABRICKS_TOKEN=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

response = ChatCompletion.create(model="databricks-dbrx-instruct",
                                messages=[{"role": "system", "content": "You are a helpful assistant."},
                                          {"role": "user","content": "What is a mixture of experts model?"}],
                                max_tokens=128)
print(f"response.message:{response.message}")

# COMMAND ----------

def get_products(search_query):

    all_column_names=['product_id', 'title', 'product_description', 'rating', 'ratings_count', 'final_price', 'currency', 'images', 'product_details', 'product_specifications', 'amount_of_stars', 'what_customers_said', 'seller_name', 'sizes', 'combined_column']
    results = index.similarity_search(
    query_text=search_query,
    columns=['product_id', 'title', 'product_description', 'rating', 'ratings_count', 'final_price', 'currency', 'images', 'product_details', 'product_specifications', 'amount_of_stars', 'what_customers_said', 'seller_name', 'sizes', 'combined_column'],
    num_results=5)

    rows = results['result']['data_array']
    df = pd.DataFrame(rows, columns=all_column_names + ['score'])
    return df

# COMMAND ----------

customer_id = '1'
product_search = "business dress"

customer_person = spark.sql(f"select * from workspace.default.customer_attributes where customer_id = '{customer_id}'")
display(customer_person)
product = get_products(product_search)
cust_df  = customer_person.toPandas()

display(cust_df)
display(product)

# COMMAND ----------


