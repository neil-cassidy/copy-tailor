# Databricks notebook source
# MAGIC %pip install openai==1.0.0

# COMMAND ----------

# MAGIC %pip install databricks-genai
# MAGIC %pip install databricks-genai-inference
# MAGIC dbutils.library.restartPython()

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

import pyspark.sql.functions as F

# COMMAND ----------

data = [
    {"customer_id" : "1" ,"age" : '25', "gender" : "Male", "location" : "Chicago", "chat" : """Customer: Hi, I'm looking for a new dress for a wedding I'm attending next week.
Chatbot: Hello! I'd be happy to help you find the perfect dress. What style and color are you looking for?
Customer: I'm not sure, something elegant and maybe in a dark blue color.
Chatbot: Great, I have a few options for you. Here's a link to a beautiful navy blue cocktail dress that would be perfect for a wedding. (Link to product)
Customer: That looks nice, do you have any customer reviews for this dress?
Chatbot: Yes, here are some reviews from other customers who have purchased this dress. (Link to reviews)
Customer: Thank you, I'll take a look and think about it.
Chatbot: You're welcome! Let me know if you have any other questions. I'm here to help.""", "reviews": """This coat is the bomb dot com! I'm a 25-year-old dude from Chicago, and I can tell you that this coat is the best. It's warm, stylish, and keeps me dry in the snow. I highly recommend it to anyone looking for a dope winter coat."""},
        
    {"customer_id" : "2" , "age" : '45', "gender" : "Female", "location" : "Dallas", "chat" :
            """Customer: Hi, I'm looking for some new jeans for work.
Chatbot: Hello! I'd be happy to help you find the right pair of jeans. What style and fit are you looking for?
Customer: I prefer slim fit jeans, and I need them in a dark wash.
Chatbot: Got it. I have a few options for you. Here's a link to a pair of slim fit jeans in a dark wash that might be what you're looking for. (Link to product)
Customer: That looks good, do you have any sizes available?
Chatbot: Yes, we have sizes 28-40 available. What size do you need?
Customer: I need a size 34.
Chatbot: Great, we have that size in stock. Would you like me to add it to your cart?
Customer: Yes, please.
Chatbot: Okay, I've added the jeans to your cart. You can check out whenever you're ready.""",
"reviews":"""I was not happy with these swimming trunks. I'm a 45-year-old dude from Dallas, and I found that the material was thin and cheap, and it became see-through when wet. I also found that the trunks did not provide enough support or coverage. I would not recommend these swimming trunks to anyone looking for a durable and high-quality swimsuit"""},
          
    { "customer_id" : "3" , "age" : '20', "gender" : "Female", "location" : "Phoenix", "chat" : """ Customer:  Hi, I'm looking for a new coat for the winter. Chatbot: Hello! I'd be happy to help you find the perfect coat. Do you have a preferred color or style? Customer: I was thinking something black and long. Chatbot: We have a few options that match your description. Here's one that's a wool coat, knee-length, and has a detachable hood. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the coat. [insert picture] Customer: That looks great. What about a scarf and gloves to go with it? Chatbot: We have a variety of scarves and gloves that would go well with that coat. Here's a black cashmere scarf and leather gloves that would complement the coat.""",
     "reviews":"""I was looking for a new swimsuit for swimming at my local pool, and I'm so glad I found this one. The swimsuit is stylish and comfortable, and it provides excellent coverage. The material is high-quality and durable, and it has held up well after multiple wears and washes. I would highly recommend this swimsuit to anyone looking for a fashionable and functional swimsuit in Phoenix"""},
           
    {"customer_id" : "4" , "age" : '25', "gender" : "Male", "location" : "Denver", "chat" :  """ Customer: Hi, I'm looking for a new dress for a formal event. Chatbot: Hello! I'd be happy to help you find the perfect dress for your formal event. Do you have a preferred color or style? Customer: I was thinking something gold and flowy. Chatbot: We have a few options that match your description. Here's one that's a maxi dress, gold, and has a V-neck. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the dress. [insert picture] Customer: That looks great. What about shoes and a clutch to go with it? Chatbot: We have a variety of shoes and clutches that would go well with that dress. Here's a pair of gold strappy sandals and a matching clutch that would complement the dress. """,
     "reviews": """I was disappointed with these sandals. I found that the sandals were not very comfortable. The straps were too tight, and the soles were not supportive. I expected better quality for the price. I wore them for a day at the beach, and by the end of the day, my feet were sore and tired. I would not recommend these sandals to anyone looking for a comfortable and supportive sandal. """},

    {"customer_id" : "5" , "age" : '65', "gender" : "Male", "location" : "Seattle", "chat" :  """ Customer: Hi, I'm looking for a new jacket for the fall. Chatbot: Hello! I'd be happy to help you find the perfect jacket for the fall. Do you have a preferred color or style? Customer: I was thinking something green and bomber style. Chatbot: We have a few options that match your description. Here's one that's a bomber jacket, green, and made of suede. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the jacket. [insert picture] Customer: That looks great. What about a scarf and a hat to go with it? Chatbot: We have a variety of scarves and hats that would go well with that jacket. Here's a green knit scarf and a matching hat that would complement the jacket. """ ,
     "reviews": """ I recently purchased this raincoat for my needs, and I have mixed feelings about it. The raincoat is lightweight and waterproof, which is great for the rainy weather in Seattle. However, I found that the fit was a bit snug, and the material was not very breathable. I would recommend this raincoat to anyone looking for a basic, waterproof option, but keep in mind that it may not be the most comfortable option for long-term wear """},

    {"customer_id" : "6" , "age" : '21', "gender" : "Female", "location" : "Detroit", "chat" :  """Customer: Hi, I'm looking for a new outfit for a night out in Detroit. Chatbot: Hello! I'd be happy to help you find the perfect outfit for a night out in Detroit. Do you have a preferred style or color? Customer: I was thinking something trendy and black. Chatbot: We have a few options that match your description. Here's a black leather jacket, a white t-shirt, and black skinny jeans. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the outfit. [insert picture] Customer: That looks great. What about shoes and accessories to go with it? Chatbot: We have a variety of shoes and accessories that would go well with that outfit. Here's a pair of black leather boots and a silver chain necklace that would complement the outfit. """ ,
     "reviews": """ I was looking for the perfect wedding dress for my big day, and I'm so glad I found this one. The dress is absolutely beautiful, with intricate details and a flattering fit. The material is high-quality and comfortable, and the dress is true to size. I received so many compliments on my wedding day, and I felt like a princess. I would highly recommend this wedding dress to any bride looking for a beautiful and elegant option """},

    {"customer_id" : "7" , "age" : '21', "gender" : "Female", "location" : "Boston", "chat" :  """ Customer: Hi, I'm looking for a new coat for the winter. Chatbot: Hello! I'd be happy to help you find the perfect coat for the winter. Do you have a preferred color or style? Customer: I was thinking something warm and stylish. Chatbot: We have a few options that match your description. Here's a parka, fur-trimmed hood, and made of down. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the coat. [insert picture] Customer: That looks great. What about a hat and gloves to go with it? Chatbot: We have a variety of hats and gloves that would go well with that coat. Here's a black knit hat and leather gloves that would complement the coat.  """ ,
     "reviews": """ These work pants are straight fire! I'm a Gen Z dude, and I can tell you that these pants are the real deal. They're comfortable, stylish, and perfect for the office. I've gotten so many compliments on them. If you're looking for a dope pair of work pants, these are it """},

    {"customer_id" : "8" , "age" : '18', "gender" : "Male", "location" : "Atlanta", "chat" :  """ Customer: Hi, I'm looking for a new pair of sneakers. Chatbot: Hello! I'd be happy to help you find the perfect pair of sneakers. Do you have a preferred brand or style? Customer: I was thinking something stylish and comfortable. Chatbot: We have a few options that match your description. Here's a pair of Nike Air Max, black and white, and made of breathable mesh. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the sneakers. [insert picture] Customer: That looks great. What about socks and a tracksuit to go with it? Chatbot: We have a variety of socks and tracksuits that would go well with those sneakers. Here's a pair of black Nike socks and a black and white Nike tracksuit that would complement the sneakers. """
      ,
     "reviews": """ I was disappointed with this shirt. I'm a Gen Z dude, and I found that the shirt was not very comfortable. The material was thin and cheap, and it became see-through when wet. I expected better quality for the price. I would not recommend this shirt to anyone looking for a durable and high-quality shirt. """ },

    {"customer_id" : "9" , "age" : '18', "gender" : "Male", "location" : "San Jose", "chat" :  """ 
    Customer: Hi, I'm looking for a new dress for a party. Chatbot: Hello! I'd be happy to help you find the perfect dress for a party. Do you have a preferred color or style? Customer: I was thinking something colorful and flowy. Chatbot: We have a few options that match your description. Here's a maxi dress, floral print, and has a V-neck. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the dress. [insert picture] Customer: That looks great. What about shoes and a clutch to go with it? Chatbot: We have a variety of shoes and clutches that would go well with that dress. Here's a pair of nude heels and a matching clutch that would complement the dress.""" },

    {"customer_id" : "10" , "age" : '31', "gender" : "Male", "location" : "San Jose", "chat" :  """ Customer: Hi, I'm looking for a new outfit for a concert in San Jose. Chatbot: Hello! I'd be happy to help you find the perfect outfit for a concert in San Jose. Do you have a preferred style or color? Customer: I was thinking something casual and comfortable. Chatbot: We have a few options that match your description. Here's a graphic t-shirt, ripped jeans, and a denim jacket. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the outfit. [insert picture] Customer: That looks great. What about shoes and accessories to go with it? Chatbot: We have a variety of shoes and accessories that would go well with that outfit. Here's a pair of white sneakers and a black leather backpack that would complement the outfit.   """ },

    {"customer_id" : "11" , "age" : '38', "gender" : "Male", "location" : "San Diego", "chat" :  """ Customer: Hi, I'm looking for a new t-shirt for a casual day out. Chatbot: Hello! I'd be happy to help you find the perfect t-shirt for a casual day out. Do you have a preferred color or style? Customer: I was thinking something simple and comfortable. Chatbot: We have a few options that match your description. Here's a white t-shirt, crew neck, and made of soft cotton. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the t-shirt. [insert picture] Customer: That looks great. What about pants and shoes to go with it? Chatbot: We have a variety of pants and shoes that would go well with that t-shirt. Here's a pair of blue jeans and white sneakers that would complement the t-shirt.""" },

    {"customer_id" : "12" , "age" : '38', "gender" : "Female", "location" : "San Antonio", "chat" :  """Customer: Hi, I'm looking for a new hoodie for a lazy day at home. Chatbot: Hello! I'd be happy to help you find the perfect hoodie for a lazy day at home. Do you have a preferred color or style? Customer: I was thinking something cozy and warm. Chatbot: We have a few options that match your description. Here's a hoodie, fleece-lined, and has a kangaroo pocket. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the hoodie. [insert picture] Customer: That looks great. What about pants and socks to go with it? Chatbot: We have a variety of pants and socks that would go well with that hoodie. Here's a pair of sweatpants and fuzzy socks that would complement the hoodie.""" },
               
    {"customer_id" : "13" , "age" : '27', "gender" : "Female", "location" : "New Orleans", "chat" :  """ Customer: Hi, I'm looking for a new outfit for a bachelorette party in New Orleans. Chatbot: Hello! I'd be happy to help you find the perfect outfit for a bachelorette party in New Orleans. Do you have a preferred style or color? Customer: I was thinking something flashy and fun. Chatbot: We have a few options that match your description. Here's a sequin dress, V-neck, and has a fit and flare silhouette. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the dress. [insert picture] Customer: That looks great. What about shoes and accessories to go with it? Chatbot: We have a variety of shoes and accessories that would go well with that dress. Here's a pair of silver strappy sandals and a matching clutch that would complement the dress.""" },

    {"customer_id" : "14" , "age" : '67', "gender" : "Male", "location" : "Oakland", "chat" :  """ Customer: Hi, I'm looking for a new hat for a sunny day in Oakland. Chatbot: Hello! I'd be happy to help you find the perfect hat for a sunny day in Oakland. Do you have a preferred style or color? Customer: I was thinking something stylish and protective. Chatbot: We have a few options that match your description. Here's a straw hat, wide brim, and has a black ribbon. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the hat. [insert picture] Customer: That looks great. What about sunglasses and a bag to go with it? Chatbot: We have a variety of sunglasses and bags that would go well with that hat. Here's a pair of aviator sunglasses and a canvas tote bag that would complement the hat. """ },

    {"customer_id" : "15" , "age" : '77', "gender" : "Male", "location" : "Raleigh", "chat" :  """ Customer: Hi, I'm looking for a new outfit for a hiking trip. Chatbot: Hello! I'd be happy to help you find the perfect outfit for a hiking trip. Do you have a preferred style or color? Customer: I was thinking something comfortable and functional. Chatbot: We have a few options that match your description. Here's a moisture-wicking shirt, long sleeves, and has a half-zip. Customer: That sounds nice. Can I see a picture? Chatbot: Of course! Here's a picture of the shirt. [insert picture] Customer: That looks great. What about pants and shoes to go with it? Chatbot: We have a variety of pants and shoes that would go well with that shirt. Here's a pair of convertible hiking pants, quick-drying, and has a built-in belt, and a pair of waterproof hiking boots that would complement the shirt.  """,
     "reviews": """ I was impressed with this cap. I'm a 77-year-old male, and I found that the cap was both comfortable and functional. I've worn it for multiple outdoor activities, such as hiking and fishing, and it has held up well. The material is high-quality and durable, and it has held up well after multiple wears and washes. I would highly recommend this cap to anyone looking for a comfortable and functional cap""" }
]

# COMMAND ----------

# df = spark.createDataFrame([data])
import pandas as pd
df = pd.DataFrame(data)

display(df)

# COMMAND ----------

# Function to call the LLM API
def get_customer_persona(customer_data):
    prompt = f"""
    You are an advanced language model that specializes in customer relationship management. Based on the provided customer information, generate a detailed customer persona including the customer's key attributes, preferences, and potential needs. Use the following attributes as input:

    Customer ID: {customer_data['customer_id']}
    Age: {customer_data['age']}
    Gender: {customer_data['gender']}
    Location: {customer_data['location']}
    Previous Chat: {customer_data['chat']}
    Previous Reviews: {customer_data['reviews']}

    Based on this, suggest customer persona like customer's likely lifestyle and interests,any product style preferences, customer demograhics like product category affinity, customer generation like genz or millenial. Don't include names or other private information


"""    
    # Calling the ChatCompletion API
    response = ChatCompletion.create(
        model="databricks-dbrx-instruct",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=128
    )

    return response.message

# COMMAND ----------

# Apply the function to each row in the DataFrame
df['persona'] = df.apply(lambda row: get_customer_persona(row), axis=1)

# Display the DataFrame with the persona column
display(df)

# COMMAND ----------

spark_df = spark.createDataFrame(df)

(
    spark_df
    .write.mode("overwrite")
    .format("delta")
    .option("mergeSchema", "true")
    .saveAsTable(
        "workspace.default.customer_attributes"
    )
)

# (spark_df.write.format("delta")
# .mode("overwrite")
# .save("bright_data_myntra_products.datasets.customer_attributes"))

# COMMAND ----------

# MAGIC %sql select * from workspace.default.customer_attributes
