from collections import Counter
import pickle
import random
import click

from dotenv import load_dotenv
import os

import numpy as np
load_dotenv()

import datetime
import logging

import sys
sys.path.append("/home/hmora/network-gen-pipeline/src")

from utils.stopwatch import Stopwatch, LoggerStopwatch
from utils.duckaccess import DuckSession
import utils.path_utils as pu

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

import duckdb

import pandas as pd

import networkx as nx

import multiprocessing as mp

from dataclasses import dataclass

from collections import defaultdict

import EoN

########################################################################################################

@dataclass
class SEIRContext:
    """ A data class used to store a SEIR simulation configuration
    """
    gamma: float = 1/14
    tau: float = 1/1.5 
    R0: float = 2.5
    Rt: float = 1.18848407391899  
    min_ei_attribute: float = 0.5  
    max_ei_attribute: float = 1.5
    min_ir_attribute: float = 0.5  
    max_ir_attribute: float = 1
    recuperados_totales: int = 362220
    infectados_totales: int = 36777 
    poblacion_total: int = 9000000


class SimulationManager:
    """
    """
    def __init__(self, network
                 , target: str
                 , logs: str
                 , seir_context: SEIRContext
                ) -> None:


        self.__target = f"{str(target).rstrip('/')}"
        
        logging.basicConfig(filename=f"{logs}/simulation_run_{datetime.datetime.now()}.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
        self.__logger = logging.getLogger(f"Simulation Run: ")

        self.__seir = seir_context

        self.__network = network

    def get_model_graph(self, g):
        """
        """
        # Not really a copy
        g_copy = g

        self.__logger.info(f"READED GRAPH")

        ei_node_attribut \
            ={node:random.uniform(a=self.__seir.min_ei_attribute
                                  , b=self.__seir.max_ei_attribute) \
                                    for node in g_copy.nodes()}
        
        self.__logger.info("COMPUTING ei_attributes")

        ir_node_attribute \
            ={node:random.uniform(a=self.__seir.min_ir_attribute
                                  , b=self.__seir.max_ir_attribute) \
                                    for node in g_copy.nodes()}

        self.__logger.info("COMPUTING ir_attributes")
        
        edge_attribute_dict \
            = {edge : 0.5 + random.random() for edge in g_copy.edges()}

        self.__logger.info("COMPUTING edge_attributes")
        
        nx.set_node_attributes(g_copy, values=ei_node_attribut, name='expose2infect_weight')
        nx.set_node_attributes(g_copy, values=ir_node_attribute, name='infect2recover_weight')
        nx.set_edge_attributes(g_copy, values=edge_attribute_dict, name='transmission_weight')

        self.__logger.info(f"NUMBER OF NODES: {g_copy.number_of_nodes()}")
        # self.__logger.info(f"\n{g_copy.nodes(data=True)}")

        self.__logger.info(f"NUMBER OF EDGES: {g_copy.number_of_edges()}")
        # self.__logger.info(f"\n{g_copy.edges(data=True)}")

        return g_copy
    
    def get_transition_graphs(self):
        """
        """
        transmission_rate = \
            1 / ( 1 / self.__seir.gamma) * self.__seir.Rt

        H = nx.DiGraph()
        H.add_node('S')
        H.add_edge('E','I', rate=self.__seir.tau, weight_label='expose2infect_weight')
        H.add_edge('I','R', rate=self.__seir.gamma, weight_label='infect2recover_weight')

        J = nx.DiGraph()
        J.add_edge(('I','S'), ('I','E'), rate=transmission_rate, weight_label='transmission_weight')

        return H, J

    def get_IC(self, g, seed):
        """
        """
        poblacion_escalada = g.number_of_nodes()

        recuperados_escalados = \
            round(self.__seir.recuperados_totales * poblacion_escalada / self.__seir.poblacion_total)

        infectados_escalados = \
            round(self.__seir.infectados_totales * poblacion_escalada / self.__seir.poblacion_total)
        
        self.__logger.info(f"INFECTADOS ESCALADOS: {infectados_escalados}")
        
        np.random.choice(seed)

        infected_collection = np.random.choice(poblacion_escalada, infectados_escalados)

        # self.__logger.info(f"INFECTED COLLECTION: {infected_collection}")

        IC = defaultdict(lambda: 'S')
        for node in infected_collection:
            IC[node] = 'I'

        # self.__logger.info(f"IC: {IC}")

        return IC
    
    def driver(self, seed):
        """
        """
        self.__logger.info("RUNNING SIMULATIONS WITH PARAMENTERS:")
        self.__logger.info(f"Gamma: {self.__seir.gamma}")
        self.__logger.info(f"Tau: {self.__seir.tau}")
        self.__logger.info(f"R0: {self.__seir.R0}")
        self.__logger.info(f"Rt: {self.__seir.Rt}")
        self.__logger.info(f"Min. EI: {self.__seir.min_ei_attribute}")
        self.__logger.info(f"Max. EI: {self.__seir.max_ei_attribute}")
        self.__logger.info(f"Min. IR: {self.__seir.min_ir_attribute}")
        self.__logger.info(f"Max. IR: {self.__seir.max_ir_attribute}")
        self.__logger.info(f"Total Recovered: {self.__seir.recuperados_totales}")
        self.__logger.info(f"Total Infected: {self.__seir.infectados_totales}")
        self.__logger.info(f"Total Population: {self.__seir.poblacion_total}")

        with LoggerStopwatch(self.__logger) as sw:

            graph = nx.convert_node_labels_to_integers(nx.read_graphml(self.__network))
            self.__logger.info(f"READING GRAPH FROM: {self.__network}")
            sw.report_til_here()

            prepared_graph = self.get_model_graph(graph)
            sw.report_til_here()

            H, J = self.get_transition_graphs()
            sw.report_til_here()

            IC = self.get_IC(graph, seed)
            sw.report_til_here()

            self.__logger.info(f"RUNNING SIMULATION")
            np.random.seed(seed)
            sim_res = EoN.Gillespie_simple_contagion(
                G = prepared_graph
                , spontaneous_transition_graph=H
                , nbr_induced_transition_graph=J
                , IC=IC
                , return_statuses=('S','E','I','R') # TODO: Add this to config
                , return_full_data=True 
                , tmax=1000 # TODO: Add this to config
            )
            sw.report_til_here()

            target = pu.create_if_not_exists(self.__target)
            with open(f"{target}/SIM_SEED_{str(seed)}_raw_result.pkl", "wb") as raw_persistence:
                pickle.dump(sim_res, raw_persistence)
            sw.report_til_here()

########################################################################################################
@click.command()
@click.argument("network_path", required=True)
@click.option('-t', "--target"
              , default=f"./simulations_dir"
              , help="""
              The target directory in which the results and side results would be written
              """)
@click.option("--logs"
              , default=f"/home/hmora/network-gen-pipeline/logs"
              , help="""
              The target directory in which the logs would be written.
              """)
@click.option("--iterative"
              , default=None
              , help="""
              The integer number of times a simulation will be generated 
              """)
@click.option("--gamma"
              , default=None
              , help="""
              """)
@click.option("--tau"
              , default=None
              , help="""
              """)
@click.option("--r0"
              , default=None
              , help="""
              """)
@click.option("--rt"
              , default=None
              , help="""
              """)
@click.option("--min_ei"
              , default=None
              , help="""
              """)
@click.option("--max_ei"
              , default=None
              , help="""
              """)
@click.option("--min_ir"
              , default=None
              , help="""
              """)
@click.option("--max_ir"
              , default=None
              , help="""
              """)
@click.option("--total_recovered"
              , default=None
              , help="""
              """)
@click.option("--total_infected"
              , default=None
              , help="""
              """)
@click.option("--total_population"
              , default=None
              , help="""
              """)
@click.option("--seed"
              , default=3696
              , help="""
              """)
def main(network_path: str, target: str, logs: str
         , iterative: int
         , gamma: float
         , tau: float
         , r0: float
         , rt: float
         , min_ei: float
         , max_ei: float
         , min_ir: float
         , max_ir: float
         , total_recovered: float
         , total_infected: float
         , total_population: float
         , seed: int
         ) -> None:
    """ Tool to run simulations under certain conditions

        NETWORK: A string indicating the path of the network over which the
        SEIR simulation must be run
    """
    seir_context = SEIRContext()

    if gamma is not None:
        seir_context.gamma = gamma

    if tau is not None:
        seir_context.tau = tau

    if r0 is not None:
        seir_context.r0 = r0

    if rt is not None:
        seir_context.rt = rt

    if min_ei is not None:
        seir_context.min_ei_attribute = min_ei

    if max_ei is not None:
        seir_context.max_ei_attribute = max_ei

    if min_ir is not None:
        seir_context.min_ir_attribute = min_ir

    if max_ir is not None:
        seir_context.max_ir_attribute = max_ir

    if total_recovered is not None:
        seir_context.recuperados_totales = total_recovered

    if total_infected is not None:
        seir_context.infectados_totales = total_infected

    if total_population is not None:
        seir_context.poblacion_total = total_population

    sim_manager = SimulationManager(network_path, target, logs, seir_context)

    sim_manager.driver(seed)

########################################################################################################

if __name__ == "__main__":

    with Stopwatch():
        main()