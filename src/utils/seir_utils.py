import networkx as nx
import matplotlib
import matplotlib.pyplot as plt
import EoN
from matplotlib import rc
import scipy
import pickle
import numpy as np
import random
import pandas as pd
import numpy as np
from collections import defaultdict
from pathlib import Path
import os

from core.context import SEIRContext

from utils.path_utils import create_if_not_exists

def funcion_preparadora(nw, context: SEIRContext):
    """
    """
    nw = nw.copy()

    # stochastic transition E-I
    ei_node_attribute={node:random.uniform(a=context.min_ei_attribute, b=context.max_ei_attribute) \
                      for node in nw.nodes()}

    # stochastic transition I-R
    # will multiply recovery rate for some factor so it is between 14 and 28 days
    ir_node_attribute={node:random.uniform(a=context.min_ir_attribute, b=context.max_ir_attribute) \
                       for node in nw.nodes()}

    #Transmission weight -varies for each pair of interactions 
    # (some contacts are very transmisibles other are not so)
    edge_attribute_dict= {edge:0.5+random.random()for edge in nw.edges()}
    nx.set_node_attributes(nw, values=ei_node_attribute, name='expose2infect_weight')
    nx.set_node_attributes(nw, values=ir_node_attribute, name='infect2recover_weight')
    nx.set_edge_attributes(nw, values=edge_attribute_dict, name='transmission_weight')

    return(nw)

def funcion_SEIR (g, context: SEIRContext, persistence_path: str):
    """
    """
    ei_rate = context.tau
    ir_rate = context.gamma
    transmission_rate= 1 / (1 / context.gamma) * context.Rt
    
    poblacion_escalada = len(g.nodes())
    
    recuperados_escalados = round(context.total_recovered * poblacion_escalada / context.total_population)
    infectados_escalados = round(context.total_infected * poblacion_escalada / context.total_population)
    
    IC = defaultdict(lambda : 'S')
    for node in range(infectados_escalados-1):
        IC[node] = 'I'
    
    for node in range(infectados_escalados, infectados_escalados+recuperados_escalados):
        IC[node]='R'
    
    return_statuses = ('S','E','I','R')
    
    H = nx.DiGraph()
    H.add_node('S')
    H.add_edge('E', 'I', rate=context.tau, weight_label='expose2infect_weight')
    H.add_edge('I', 'R', rate=context.gamma, weight_label='infect2recover_weight')
    
    J = nx.DiGraph()
    J.add_edge(('I','S'), ('I','E'), rate=transmission_rate, weight_label='transmission_weight')
    
    np.random.seed(context.seed)
    random.seed(context.seed)
    prep_g = funcion_preparadora(g, context)
    mod_nuloAbs = \
        EoN.Gillespie_simple_contagion(G=prep_g
                                       , spontaneous_transition_graph=H
                                       , nbr_induced_transition_graph=J
                                       , IC=IC
                                       , return_statuses=return_statuses
                                       , return_full_data=True
                                       , tmax=1000)
    
    predata = mod_nuloAbs.summary()[1]
    predata["t"] = mod_nuloAbs.summary()[0]
    df = pd.DataFrame(predata)
    df = df.assign(I_pc=100 * df['I'] / poblacion_escalada)
    
    
    df2 = pd.DataFrame(columns=['S','E','I','R'])
    for a in range(len(g.nodes())):
        df2 = \
            pd.concat([df2
                       , pd.DataFrame(mod_nuloAbs.node_history(a), columns=mod_nuloAbs.node_history(a)[1]).iloc[0:1]],ignore_index=True)
        
    if not os.path.exists(f"{persistence_path}/seir_context.pkl"):
        with open(f"{persistence_path}/seir_context.pkl", "wb") as context_persistence:
            pickle.dump(context, context_persistence)
        
    if not os.path.exists(f"{persistence_path}/raw_result.pkl"):
        with open(f"{persistence_path}/raw_result.pkl", "wb") as raw_persistence:
            pickle.dump(mod_nuloAbs, raw_persistence)

    if not os.path.exists(f"{persistence_path}/H_digraph.pkl"):
        with open(f"{persistence_path}/H_digraph.pkl", "wb") as h_persistence:
            pickle.dump(H, h_persistence)

    if not os.path.exists(f"{persistence_path}/J_digraph.pkl"):
        with open(f"{persistence_path}/J_digraph.pkl", "wb") as j_persistence:
            pickle.dump(J, j_persistence)

    return(df.plot(x="t", y="I_pc", figsize=(8,5)), df, df2)

def run_simulation(payload):
    """
    """
    path_to_graph, context = payload
    g = nx.convert_node_labels_to_integers(nx.read_graphml(path_to_graph))
    year_path, month_path, day_path, name_file = \
        path_to_graph.strip("/datos/EpiTeam/redes/").split('/') 
    name_file = name_file.strip(".graphml")

    output = \
        create_if_not_exists(
            f"/datos/EpiTeam/simulaciones_redes/{year_path}/{month_path}/{day_path}/{name_file}" 
        )
    
    persistence_path = \
        create_if_not_exists(
            f"{output}/insumos_y_persistencia" 
        )

    for i in range (0,100):
        np.random.seed(i)
                        
        Resultado_SEIR = funcion_SEIR(g, context, persistence_path)
        
        Resultado_SEIR[0].figure.savefig(f"{output}/{name_file}_{str(i)}_grafico.pdf")
        Resultado_SEIR[1].to_csv(f"{output}/{name_file}_{str(i)}_tabla_de_grafica.csv")
        Resultado_SEIR[2].to_csv(f"{output}/{name_file}_{str(i)}_node_status.csv")
    
        matplotlib.pyplot.close()

    return