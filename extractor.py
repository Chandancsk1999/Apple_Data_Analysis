# Databricks notebook source
class extractor:
    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(extractor):
    def extract(self):
        transactionDf=get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()
        customerDf=get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/Customer_Updated.csv"
        ).get_data_frame()
        customerDf.show()

        transactionDf.show()
        inputDf={"transactionDf":transactionDf,"customerDf":customerDf}
        return inputDf

# COMMAND ----------

# inputDf.get("transactionDf").show()