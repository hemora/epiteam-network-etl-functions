from dotenv import load_dotenv
import os
load_dotenv()

import sys

from utils.path_utils import create_if_not_exists

class Context:
    """ Context class to propagate year, month and day info
    """
    def __init__(self, year: str, month: str, day: str):
        self.__year = year
        self.__month = month
        self.__day = day
        self.__base_dir = \
            create_if_not_exists(
                f"/datos/EpiTeam/insumos_redes/year={year}/month={month}/day={day}"
            )
        self.__payload = None

    @property
    def year(self):
        return self.__year
    @year.setter
    def year(self, value):
        self.__year = value

    @property
    def month(self):
        return self.__month
    @month.setter
    def month(self, value):
        self.__month = value

    @property
    def day(self):
        return self.__day
    @day.setter
    def day(self, value):
        self.__day = value

    @property
    def base_dir(self):
        return self.__base_dir
    @base_dir.setter
    def base_dir(self, value):
        self.__base_dir = value

    @property
    def payload(self):
        return self.__payload
    @payload.setter
    def payload(self, value):
        self.__payload = value
    
class ExtractContext(Context):
    """
    """
    def __init__(self, year: str, month: str, day: str):
        super().__init__(year, month, day)
        self.__data_source = os.environ[f"MOVILIDAD_RAW_{year}"] \
            if year in ["2020", "2021"] else sys.exit("Invalid Year")
        self.__raw_pings_target = os.environ[f"RAW_PINGS_TARGET"]
        self.__payload = None
    
    @property
    def data_source(self):
        return self.__data_source
    @data_source.setter
    def data_source(self, value):
        self.__data_source = value

    @property
    def raw_pings_target(self):
        return self.__raw_pings_target
    @raw_pings_target.setter
    def payload(self, value):
        self.__raw_pings_target = value
    
    @property
    def payload(self):
        return self.__payload
    @payload.setter
    def payload(self, value):
        self.__payload = value

class TransformContext(ExtractContext):
    """
    """
    def __init__(self, year: str, month: str, day: str, only_if_exists: bool = True):
        super().__init__(year, month, day)
        self.__ageb_catalog = os.environ["AGEB_CATALOG"]
        self.__ntl_pings_target = os.environ["NTL_PINGS_TARGET"]
        self.__only_if_exists = only_if_exists

    @property
    def ageb_catalog(self):
        return self.__ageb_catalog
    @ageb_catalog.setter
    def ageb_catalog(self, value):
        self.__ageb_catalog = value
    
    @property
    def ntl_pings_target(self):
        return self.__ntl_pings_target
    @ntl_pings_target.setter
    def ntl_pings_target(self, value):
        self.__ntl_pings_target = value
    
    @property
    def only_if_exists(self):
        return self.__only_if_exists
    @only_if_exists.setter
    def only_if_exists(self, value):
        self.__only_if_exists = value
    
class InteractionsContext(Context):
    """
    """
    def __init__(self, year: str, month: str, day: str, in_vm: bool = True):
        super().__init__(year, month, day)
        self.__in_vm = in_vm
        self.__pings_base = os.environ["LOCATED_PINGS_TARGET"]
        self.__payload = None
    
    @property
    def in_vm(self):
        return self.__in_vm
    @in_vm.setter
    def in_vm(self, value):
        self.__in_vm = value
    
    @property
    def pings_base(self):
        return self.__pings_base
    @pings_base.setter
    def pings_base(self, value):
        self.__pings_base = value
    
    @property
    def payload(self):
        return self.__payload
    @payload.setter
    def payload(self, value):
        self.__payload = value

class MatrixContext(InteractionsContext):
    """
    """
    def __init__(self, year: str, month: str, day: str, in_vm: bool = True):
        super().__init__(year, month, day, in_vm)
        self.__interactions = os.environ["INTERACTIONS_TABLE"]
    
    @property
    def interactions(self):
        return self.__interactions
    @interactions.setter
    def interactions(self, value):
        self.__interactions = value

class SEIRContext(Context):

    def __init__(self, year: str, month: str, day: str):
        super().__init__(year, month, day)
        self.__base_dir = \
            create_if_not_exists(
                f"/datos/EpiTeam/simulaciones_redes/year={year}/month={month}/day={day}"
            )
        
        self.__gamma = float(os.environ["GAMMA"])
        self.__tau = float(os.environ["TAU"])
        self.__R0 = float(os.environ["R0"])
        self.__Rt = float(os.environ["RT"])
        self.__total_recovered = int(os.environ["TOTAL_RECOVERED"])
        self.__total_infected = int(os.environ["TOTAL_INFECTED"])
        self.__total_population = int(os.environ["TOTAL_POPULATION"])
        self.__min_ei_attribute = float(os.environ["MIN_EI_ATTR"])
        self.__max_ei_attribute = float(os.environ["MAX_EI_ATTR"])
        self.__min_ir_attribute = float(os.environ["MIN_IR_ATTR"])
        self.__max_ir_attribute = float(os.environ["MAX_IR_ATTR"])
        self.__seed = int(os.environ["SEIR_SEED"])


    @property
    def gamma(self):
        return self.__gamma
    @gamma.setter
    def gamma(self, value):
        self.__gamma = value
    
    @property
    def tau(self):
        return self.__tau
    @tau.setter
    def tau(self, value):
        self.__tau = value
    
    @property
    def R0(self):
        return self.__R0
    @R0.setter
    def R0(self, value):
        self.__R0 = value
    
    @property
    def Rt(self):
        return self.__Rt
    @Rt.setter
    def Rt(self, value):
        self.__Rt = value

    @property
    def min_ei_attribute(self):
        return self.__min_ei_attribute
    @min_ei_attribute.setter
    def min_ei_attribute(self, value):
        self.__min_ei_attribute = value

    @property
    def max_ei_attribute(self):
        return self.__max_ei_attribute
    @max_ei_attribute.setter
    def max_ei_attribute(self, value):
        self.__max_ei_attribute = value
    
    @property
    def min_ir_attribute(self):
        return self.__min_ir_attribute
    @min_ir_attribute.setter
    def min_ir_attribute(self, value):
        self.__min_ir_attribute = value
    
    @property
    def max_ir_attribute(self):
        return self.__max_ir_attribute
    @max_ir_attribute.setter
    def max_ir_attribute(self, value):
        self.__max_ir_attribute = value

    @property
    def total_recovered(self):
        return self.__total_recovered
    @total_recovered.setter
    def total_recovered(self, value):
        self.__total_recovered = value
    
    @property
    def total_infected(self):
        return self.__total_infected
    @total_infected.setter
    def total_infected(self, value):
        self.__total_infected = value
    
    @property
    def total_population(self):
        return self.__total_population
    @total_population.setter
    def total_population(self, value):
        self.__total_population = value

    @property
    def seed(self):
        return self.__seed
    @seed.setter
    def seed(self, value):
        self.__seed = value