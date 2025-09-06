# Databricks notebook source
class Transformer:
    def __init__(self):
        pass
    def transform(self,inputDf):
        pass

class AirpodsAfterIphone(Transformer):
    def transform(self,inputDf):
        """  Customer who have baught Airpods after buying iphones  """
        transformedDf=inputDf.get("transactionDf")
        customerDf=inputDf.get("customerDf")
        
        window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")


        transformedDf=transformedDf.withColumn("next_product",lead("product_name").over(window_spec))
        finalDf=transformedDf.filter((col("product_name")=='iPhone') & (col("next_product")=='AirPods'))
        print("Person Buying Airpods after Buying Iphones")

        # finalDf1=finalDf.join(customerDf,finalDf["customer_id"]==customerDf["customer_id"],how='inner')
        finalDf1=finalDf.join(broadcast(customerDf),"customer_id")
        finalDf1.select("customer_id","customer_name","location").show()
        return finalDf1.select("customer_id","customer_name","location")
    
class AirpodsAndIphone(Transformer):
    def transform(self,inputDf):
        transactiondf=inputDf.get("transactionDf")
        customerdf=inputDf.get("customerDf")

        transformedDf=transactiondf.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        transformedDf.show()

        finalDf=transformedDf.filter(
            (col("products").array_contains("iphone")) &
            (col("products").array_contains("Airpods"))&
            (size(col("products")) == 2)
        )

        print("Person Buying Only Airpods & Iphones")

        # finalDf1=finalDf.join(customerDf,finalDf["customer_id"]==customerDf["customer_id"],how='inner')
        finalDf1=finalDf.join(broadcast(customerDf),"customer_id")
        finalDf1.select("customer_id","customer_name","location").show()
        print("Person Buying Only Airpods & Iphones")
        return finalDf1.select("customer_id","customer_name","location")


        

# COMMAND ----------

