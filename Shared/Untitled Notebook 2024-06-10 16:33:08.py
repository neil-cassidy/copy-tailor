# Databricks notebook source
df = spark.sql("select * from bright_data_myntra_products.datasets.myntra_products_1")
display(df)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col, regexp_replace

combined_col = concat_ws(" ", col("title"), col("product_description"), col("product_details"),
                         col("sizes"))

# Remove punctuation [{]} using regexp_replace
cleaned_df = df.withColumn("combined_column", regexp_replace(combined_col, "[\[\]{},]", ""))


display(cleaned_df)

# COMMAND ----------

(
    cleaned_df
    .write.mode("overwrite")
    .format("delta")
    .saveAsTable(
        "workspace.default.product_details_1"
    )
)

# COMMAND ----------

# MAGIC %sql select * from workspace.default.product_details_1

# COMMAND ----------


df1 = spark.sql("select * from workspace.default.product_details_1")

x = ['product_id', 'title', 'product_description', 'rating', 'ratings_count', 'final_price', 'currency', 'images', 'product_details', 'product_specifications', 'amount_of_stars', 'what_customers_said', 'seller_name', 'sizes', 'combined_column']

df2 = df1.select(*x)
display(df2)

(
    df2
    .write.mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "True")
    .saveAsTable(
        "workspace.default.product_details_2"
    )
)

# COMMAND ----------

# MAGIC %sql select * from workspace.default.product_details_2

# COMMAND ----------

spark_df.write.format("delta").mode("overwrite").save("bright_data_myntra_products.datasets.customer_attributes")
