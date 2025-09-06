# Databricks notebook source
#Abstract Class
class DataSource:
    def __init__(self,path):
        self.path=path

    def get_data_frame(self):
        #Abstract Method, Function will be defined in subclass

        raise ValueError("Not Implemented")

class CSVDataSource(DataSource):
    def get_data_frame(self):
        return(
            spark.
            read.
            format("csv").
            option('header','True').
            load(self.path)
        )

class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return(
            spark.
            read.
            format("parquet").
            load(self.path)
        )

class DeltaDataSorce(DataSource):
    def get_data_frame(self):
        return(
            spark.
            read.
            format("delta").
            load(self.path)
        )
def get_data_source(data_type,file_path):
    if data_type=="csv":
        return CSVDataSource(file_path)
    elif data_type=="parquet":
        return ParquetDataSource(file_path)
    elif data_type=="Delta":
        return DeltaDataSorce(file_path)
    else:
        print("Invalid File Format")



# COMMAND ----------

