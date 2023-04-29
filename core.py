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

import random
from abc import ABCMeta, abstractmethod

import pyspark
from pyspark.sql import SparkSession

from dataclasses import dataclass

from ntl_functions import NTLClass

def create_if_not_exists(path: str):
    if not os.path.isdir(path):
        os.makedirs(path)
    
    return path

def escalamiento(df, tam_final):
    N = df.select(F.sum("cardinalidad").alias("whole_sum")).collect()[0]["whole_sum"]

    out = df.withColumn("escalado", ( col("cardinalidad") * lit(tam_final) ) / lit(N) ) \
        .withColumn("ceil", F.ceil("escalado")) \
        .withColumn("rounded", F.ceil(F.round("escalado")))

    pandas_aux = out.toPandas()
    scale_acc = list(pandas_aux["rounded"])

    for index, row in pandas_aux.iterrows():
        if row["ceil"] > 0 and scale_acc[index] == 0 :
            scale_acc[index] = 1
    
        if sum(scale_acc) == 10000:
            break

    only_ageb_tags = list(pandas_aux["home_ageb"])

    return scale_acc, list(zip(only_ageb_tags, scale_acc))

class SparkAccess:
    "Clase para proveer de acceso a Spark a las clases manejadoras"
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .config("spark.jars", "./utils/postgresql-42.5.1.jar") \
            .master("local[40]") \
            .config("spark.driver.memory", "50g") \
            .config("spark.executor.memory", "50g") \
            .appName("Generacion de Redes") \
            .getOrCreate()

    def get_session(self):
        return self.spark

@dataclass
class FilePathPayload:
    spark: any
    day: str
    month: str
    year: str

@dataclass
class FilteredPayload:
    spark: any
    day: str
    month: str
    year: str
    df: pyspark.sql.dataframe.DataFrame

@dataclass
class HomeAgebPayload:
    spark: any
    df: pyspark.sql.dataframe.DataFrame

@dataclass
class GenPayload:
    spark: any
    df: pyspark.sql.dataframe.DataFrame = None
    pings_table: pyspark.sql.dataframe.DataFrame = None
    interactions_table: pyspark.sql.dataframe.DataFrame = None
    day: str = None
    month: str = None
    year: str = None
    enlaces: any = None
    observados: any = None
    sizes: any = None


class IHandler(metaclass=ABCMeta):
    "Interfaz de la que deben de heredar todas las clases transformadoras"
    @staticmethod
    @abstractmethod
    def handle(payload):
        "A method to implement"

class Successor1(IHandler):
    "A Concrete Handler"
    @staticmethod
    def handle(payload):
        print(f"Successor1 payload = {payload}")
        test = random.randint(1, 2)
        if test == 1:
            payload = payload + 1
            payload = Successor1().handle(payload)
        if test == 2:
            payload = payload - 1
            payload = Successor2().handle(payload)
        return payload

class Successor2(IHandler):
    "A Concrete Handler"
    @staticmethod
    def handle(payload):
        print(f"Successor2 payload = {payload}")
        test = random.randint(1, 3)
        if test == 1:
            payload = payload * 2
            payload = Successor1().handle(payload)
        if test == 2:
            payload = payload / 2
            payload = Successor2().handle(payload)
        return payload

class InitialFilter(IHandler, SparkAccess):
    "Handler para el primer filtrado de los datos"
    @staticmethod
    def handle(payload: FilePathPayload):
        initial_df = payload.spark.read.option("header", True) \
            .parquet(f'{os.environ[f"MOVILIDAD_RAW_{payload.year}"]}/month={payload.month}/day={payload.day}') \
            .distinct() \
            .where(col("horizontal_accuracy") >= lit(100.0)) \
            .withColumn("utc_datetime", F.from_unixtime(col("utc_timestamp"))) \
            .withColumn("cdmx_datetime", F.from_utc_timestamp(col("utc_datetime"), "America/Mexico_City")) \
            .select("utc_timestamp", "cdmx_datetime", "caid", "latitude", "longitude", "horizontal_accuracy")
        
        return NTLHandler().handle(GenPayload(payload.spark, day = payload.day, month = payload.month, year = payload.year, df = initial_df))
        #return FilteredPayload(payload.spark, initial_df)

class NTLHandler(IHandler):
    "Handler para el correcto procesamiento de los NTL"
    @staticmethod
    def handle(payload: FilteredPayload):

        ntl_class = NTLClass()

        home_agebs_catalog = ntl_class.last_n_days_ntl(
            int(payload.day), int(payload.month), int(payload.year)
            )

        pings_table_pre = payload.df.join(home_agebs_catalog
                , payload.df.caid == home_agebs_catalog.caid
                , how="left") \
            .withColumn("h3index_5", h3_pyspark.geo_to_h3("latitude", "longitude", lit(5))) \
            .withColumn("h3index_12", h3_pyspark.geo_to_h3("latitude", "longitude", lit(12))) \
            .withColumn("h3index_15", h3_pyspark.geo_to_h3("latitude", "longitude", lit(15))) \
            .withColumn("home_ageb", F.when(~F.isnull(col("cve_geo")), col("cve_geo")).otherwise(lit("0000000000000"))) \
            .withColumn("time_window", F.window("cdmx_datetime", "600 seconds")) \
            .select("utc_timestamp", "cdmx_datetime", payload.df.caid, "home_ageb", "h3index_5", "h3index_12", "h3index_15", "time_window")
        
        getting_ageb_df = ntl_class.agebs_vm.select("cve_geo", "geometry") \
            .withColumn("h3polyfill_5", index_shape("geometry", lit(5))) \
            .withColumn("h3polyfill_5", F.explode("h3polyfill_5")) \
            .withColumn("h3polyfill_12", index_shape("geometry", lit(12))) \
            .withColumn("h3polyfill_12", F.explode("h3polyfill_12"))

        pings_table = pings_table_pre.join(getting_ageb_df
                , pings_table_pre.h3index_5 == getting_ageb_df.h3polyfill_5
                , how="left") \
            .where(
                (col("h3index_12") == col("h3polyfill_12")) 
            ) \
            .select("time_window", "caid", "home_ageb"
                , col("cve_geo").alias("curr_geo"), "h3index_15") \
            .distinct()

        pings_table.persist().count()

        payload.pings_table = pings_table

        return InteractionsHandler().handle(payload)

class InteractionsHandler(IHandler):
    "Handler para el correcto procesamiento de los NTL"
    @staticmethod
    def handle(payload: GenPayload):
        interactions_base = payload.pings_table.alias("a").join(payload.pings_table.alias("b")
            , (col("a.h3index_15") == col("b.h3index_15")) & (col("a.time_window") == col("b.time_window"))
            , how="inner"
        ) \
        .where(col("a.caid") != col("b.caid")) \
        .select(col("a.caid").alias("caid_a"), col("a.home_ageb").alias("home_ageb_a")
            , col("b.caid").alias("caid_b"), col("b.home_ageb").alias("home_ageb_b")
        ) \
        .distinct()

        payload.interactions_table = interactions_base

        return MatrixHandler().handle(payload)

class MatrixHandler(IHandler):
    "Handler para el correcto procesamiento de los NTL"
    @staticmethod
    def handle(payload: GenPayload):
        ageb_sizes = payload.pings_table.groupBy("home_ageb") \
            .agg(F.count_distinct("caid").alias("cardinalidad")) \
            .orderBy(col("home_ageb").asc())
        
        enlaces_posibles = ageb_sizes.alias("a").crossJoin(ageb_sizes.alias("b")) \
            .withColumn("total_enlaces", F.ceil((col("a.cardinalidad") * col("b.cardinalidad")))) \
            .select(col("a.home_ageb").alias("bloque_a"), col("b.home_ageb").alias("bloque_b"), "total_enlaces") \
            .orderBy(col("bloque_a").asc(), col("bloque_b").asc())
        
        interacciones_observadas = payload.interactions_table.groupBy("home_ageb_a", "home_ageb_b").count() \
            .withColumnRenamed("count", "no_contactos") \
            .orderBy(col("home_ageb_a").asc(), col("home_ageb_b").asc())

        observados_vs_posibles_pre = enlaces_posibles.alias("a").join(interacciones_observadas.alias("b")
                                , (enlaces_posibles.bloque_a == interacciones_observadas.home_ageb_a) 
                                    & (enlaces_posibles.bloque_b == interacciones_observadas.home_ageb_b)
                                , how="left"
                            )
        
        observados_vs_posibles = observados_vs_posibles_pre.withColumn("no_contactos", F.when(F.isnull("no_contactos"), lit(0)).otherwise(col("no_contactos"))) \
            .select(col("bloque_a").alias("bloque_a"), col("bloque_b").alias("bloque_b"), "total_enlaces", "no_contactos") \
            .orderBy(col("bloque_a").asc(), col("bloque_b").asc())

        enlaces_totales_df = observados_vs_posibles.select("bloque_a", "bloque_b", "total_enlaces") \
            .orderBy(col("bloque_a").asc(), col("bloque_b").asc())
        pivoted_enlaces = enlaces_totales_df.groupby("bloque_a") \
            .pivot("bloque_b") \
            .sum("total_enlaces")

        output = create_if_not_exists(f"/datos/EpiTeam/redes/year={payload.year}/month={payload.month}/day={payload.day}/matrices_bloques/")

        matriz_enlaces = pivoted_enlaces.toPandas()
        matriz_enlaces = matriz_enlaces.sort_values(by="bloque_a") # <---
        payload.enlaces = matriz_enlaces

        matriz_enlaces.to_csv(f"{output}/matriz_enlaces.csv")


        enlaces_obs_df = observados_vs_posibles.select("bloque_a", "bloque_b", "no_contactos").orderBy(col("bloque_a").asc(), col("bloque_b").asc())
        pivoted_obs = enlaces_obs_df.groupby("bloque_a").pivot("bloque_b").sum("no_contactos")

        matriz_obs = pivoted_obs.toPandas()
        matriz_obs = matriz_obs.sort_values(by="bloque_a") # <---
        payload.observados = matriz_obs

        matriz_obs.to_csv(f"{output}/matriz_obs.csv")

        sizes = ageb_sizes.orderBy(col("home_ageb").asc())
        sizes_final = sizes.toPandas() # <---
        payload.sizes = sizes_final

        sizes_final.to_csv(f"{output}/sizes.csv")

        return payload

class NetworkHandlerOnlyExistingAgebs(IHandler):
    "Handler para el correcto procesamiento de las matrices de enlaces"
    @staticmethod
    def handle(payload: GenPayload):

        matriz_enlaces_info = payload.enlaces.drop(columns=["Unnamed: 0", "bloque_a", "0000000000000"]).tail(-1)
        matriz_enlaces_final = matriz_enlaces_info.to_numpy()

        matriz_obs_info = payload.observados.drop(columns=["Unnamed: 0", "bloque_a", "0000000000000"]).tail(-1)
        matriz_obs_final = matriz_obs_info.to_numpy()

        matriz_probs = matriz_obs_final / matriz_enlaces_final

        sizes_df = payload.spark.createDataFrame(payload.sizes.drop(["Unnamed: 0"], axis=1).tail(-1))

        esc, zipped = escalamiento(sizes_df, 10000)
        assert sum(esc) == 10000

        output = create_if_not_exists(f"/datos/EpiTeam/redes/year={payload.year}/month={payload.month}/day={payload.day}")

        for i in range(305, 406):

            sbm = nx.stochastic_block_model(esc, matriz_probs, seed=i)

            aux_columns = ["home_ageb", "cardinalidad"]
            ageb_tags_pre = payload.spark.createDataFrame(zipped, aux_columns)

            ageb_tags = ageb_tags_pre.orderBy(col("home_ageb").asc()) \
                .withColumn("tags", F.array_repeat(col("home_ageb"), col("cardinalidad").cast(IntegerType()))) \
                .withColumn("final_tags", F.explode("tags")) \
                .select("final_tags")

            ageb_tags_pdf = ageb_tags.toPandas()
            tags_final = list(ageb_tags_pdf["final_tags"])

            assert len(tags_final) == sum(esc)

            n=len(list(sbm.nodes())) ##lista con los todos los nodos del modelo generado
            enlaces=list(sbm.edges())
            bloque= nx.get_node_attributes(sbm, "block") ##diccionario con keys:nombre nodo, value: bloque al que pertenece el nodo
            bloque= list(bloque.values()) ## lista con solo los values

            ##nuevo diccionario
            test_keys=list(sbm.nodes())
            nuevo_diccionario =  {test_keys[i]: tags_final[i] for i in range(len(test_keys))}

            ##Con la info de arriba se hace el nuevo grafo que ya puede escribirse en graphml
            g2=nx.empty_graph(n) ##le pongo los mismos nodos
            g2.add_edges_from(enlaces) ##le pongo los mismos enlaces
            nx.set_node_attributes(g2, nuevo_diccionario, "block") ##le agrego un atributo (bloque) a cada nodo

            ##Escribo el graphml
            nx.write_graphml(g2, f'{output}/{payload.day}_{payload.month}_{payload.year}_SEED_{i}.graphml')
        
        return









        
class Chain():
    "A chain with a default first successor"
    @staticmethod
    def start(payload):
        "Setting the first successor that will modify the payload"
        return InitialFilter().handle(payload)