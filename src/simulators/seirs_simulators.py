from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context

import os
from pathlib import Path
import utils.seir_utils as seir

import multiprocessing as mp

class SEIRSimulation(AbstractHandler):

    def simulate(self, context: Context):

        network_path = f"/datos/EpiTeam/redes/year={context.year}/month={context.month}/day={context.day}"

        graph_list = [(str(a), context) for a in Path(network_path).glob('*.graphml')]

        with mp.Pool(5) as p:
            p.map(seir.run_simulation, graph_list)

        context.payload = graph_list

        return context
        

    def handle(self, request: Any) -> Any:
        return super().handle(self.simulate(request))