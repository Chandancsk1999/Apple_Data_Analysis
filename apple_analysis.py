# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

# MAGIC %run "./transform"
# MAGIC

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import lead,col,broadcast,collect_set,size

# COMMAND ----------

from pyspark.sql.functions import col
class First_work_flow:
    """ETL for customer who have bought Airpods After Iphone"""
    def __init__(self):
        pass

    def runner(self):
        # Step-1-Extract All Required data from different source
        inputDf=AirpodsAfterIphoneExtractor().extract()

        # Step-2-Implement the transformation logic
        firstTransform=AirpodsAfterIphone().transform(inputDf)

        # Step-3-Loading to Different Data Sources
        AirPodsAfterIphoneLoader(firstTransform).sink()


    

# COMMAND ----------



# COMMAND ----------

class Second_workFlow:
    """ETL for Customers who have bought Iphone and Airpods Only"""
    def __init__(self):
        pass

    def runner(self):
        # Step_1-Extract all required files from differnet data source
        inputDf=AirpodsAfterIphoneExtractor().extract()
        # Step_2-Implement the transformation logic for customer who have bought Airpods and Iphone Only
        secondransform=AirpodsAndIphone().transform(inputDf)
        #Step-3-Load all required data to different sink
        OnlyAirPodsAndIphoneLoader(secondransform).sink()


# COMMAND ----------

class work_flow_runner:
    def __init__(self,name):
        self.name=name

    def  runner(self):
        if self.name=="First_work_flow":
            return First_work_flow().runner()
        
        elif self.name=="Second_workFlow":
            return Second_workFlow().runner()
        
        else:
            print("Invalid ETL Calling")

name="Second_workFlow"
work_flow_runner=work_flow_runner(name).runner()


# COMMAND ----------

