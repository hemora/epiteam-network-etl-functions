from pyspark.sql import SparkSession

class SparkAccess:
    """
    """
    def __init__(self) -> None:
        self.__spark = SparkSession.builder \
            .master("local[*]") \
            .config("spark.driver.memory", "50g") \
            .config("spark.executor.memory", "50g") \
            .getOrCreate()
        
        sc = self.__spark.sparkContext
        sc.setLogLevel("WARN")
        
    def __enter__(self):
        return self.__spark
    
    def __exit__(self, exception_type, exception_value, traceback):
        self.__spark.stop()