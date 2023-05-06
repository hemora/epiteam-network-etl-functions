from extractors.ParquetExtractor import ParquetExtractor
from transforms.NTLTransforms import NTLPreparation, NTLCountWinner, NTLJoiner
from transforms.InteractionsTransforms import InteractionsCompute, VMFilter
from transforms.MatrixTransforms import SizesMatrix, TotalEdgesMatrix, ObservedMatrix, TotalVsObservedMatrix, ProbAndSizesMatrix
from transforms.LocalizationTransform import LocalizationTransform
from wrappers.SparkWrapper import SparkWrapper
from core.context import Context
from pyspark.sql import functions as F

import matplotlib.pyplot as plt

import numpy as np

import networkx as nx

from dotenv import load_dotenv
import os
load_dotenv()


if __name__ == "__main__":

    sc = SparkWrapper("Test")
    ctxt = Context("2020", "01", "16", sc.get_session(), "test.logs")

    #print(ctxt.year)
    #print(ctxt.month)
    #print(ctxt.day)
    #print(ctxt.parquet_path)
    #print(ctxt.ageb_catalog)
    #print(type(ctxt.logger))

    pe = ParquetExtractor()
    ntl1 = NTLPreparation()
    ntl2 = NTLCountWinner()
    ntl3 = NTLJoiner()

    pe.set_next(ntl1).set_next(ntl2).set_next(ntl3)

    #result = pe.handle(ctxt)

    ic0 = LocalizationTransform()

    #result = ic0.handle(ctxt)

    #ic0 = VMFilter()
    ic1 = InteractionsCompute()
    mt1 = SizesMatrix()
    mt2 = TotalEdgesMatrix()
    mt3 = ObservedMatrix()
    mt4 = TotalVsObservedMatrix()
    mt5 = ProbAndSizesMatrix()

    #ic1.set_next(mt1).set_next(mt2).set_next(mt3).set_next(mt4)
    
    #result = ic1.handle(ctxt)

    result = mt5.handle(ctxt)

    #print(type(result.df))
    sizes, matriz_probs = result.df

    print(len(sizes))
    print(matriz_probs.shape)

    sbm_original = nx.stochastic_block_model(sizes, matriz_probs)

    A_original = list(dict(sbm_original.degree).values())

    prob_a, bins_a = np.histogram(A_original)
    prob_a = prob_a/len(A_original)
    plt.title(f"Tamaño original {sum(sizes)} nodos")
    plt.bar(bins_a[:-1], prob_a)
    plt.savefig("./images/distribucion_de_grado.png")

    plt.clf()

    sbm_aux_original = nx.adjacency_matrix(sbm_original)
    plt.title(f"Tamaño original {sum(sizes)} nodos")
    plt.spy(sbm_aux_original)
    plt.savefig("./images/matriz_adyacencias.png")

    plt.clf()

    #nx.draw(sbm_original)
    #plt.savefig("./images/graph.png")




    #pandas_result = result.home_ageb_catalog.toPandas()

    #print(pandas_result.shape)
    #print(pandas_result.dtypes)
    #print(pandas_result.columns)

    #print(result.home_ageb_catalog.show(10))
    #print(result.select(F.to_date(F.col("cdmx_datetime"))).distinct().show(10))
