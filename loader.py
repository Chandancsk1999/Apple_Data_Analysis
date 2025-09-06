# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class load:
    def __init__(self,transformdf):
        self.transformdf=transformdf

    def sink(self):
        pass

class AirPodsAfterIphoneLoader(load):
    def sink(self):
        get_sink_source(
            sink_type="dbfs",
            df=self.transformdf,
            path="/Workspace/airpodsAfterIphone",
            method="overwrite"
        ).load_data_frame()


class OnlyAirPodsAndIphoneLoader(load):
    def sink(self):
        params={
            "partionByColumns":["location"]
        }
        get_sink_source(
            sink_type="dbfs_with_partition",
            df=self.transformdf,
            path="/Workspace/OnlyAirpodsAndIphone",
            method="overwrite",
            params=params
        ).load_data_frame()

