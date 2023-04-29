from dotenv import load_dotenv
import os
load_dotenv()

from functools import reduce

import json

from h3 import h3

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, lit, to_date, when, array
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, ArrayType, IntegerType
from h3_pyspark.indexing import index_shape
import h3_pyspark
from pyspark.sql.window import Window

from datetime import timedelta, date

import numpy as np
import matplotlib.pyplot as plt
import networkx as nx

from core import Chain, FilePathPayload


if __name__ == "__main__":

    spark = SparkSession.builder \
                .config("spark.jars", "./utils/postgresql-42.5.1.jar") \
                .master("local[40]") \
                .config("spark.driver.memory", "50g") \
                .config("spark.executor.memory", "50g") \
                .appName("Generacion de Redes") \
                .getOrCreate()

    # The Client
    CHAIN = Chain()
    PAYLOAD = FilePathPayload(spark, "04", "02", "2020")
    OUT = CHAIN.start(PAYLOAD)

    print(f"Finished result = {type(OUT)}")

    #OUT..printSchema()
    #OUT.interactions_table.show(10)

    print(OUT.enlaces.columns)
    print(OUT.observados.columns)
    print(OUT.observados.columns)