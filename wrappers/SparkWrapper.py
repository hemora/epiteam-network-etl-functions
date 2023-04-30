import pyspark
from pyspark.sql import SparkSession

class SparkWrapper:
    """
    """
    def __init__(self, name: str) -> None:
        """
        """
        self.spark = SparkSession.builder \
            .config("spark.jars", "./utils/postgresql-42.5.1.jar") \
            .master("local[*]") \
            .config("spark.driver.memory", "50g") \
            .config("spark.executor.memory", "50g") \
            .appName(name) \
            .getOrCreate()

    def get_session(self):
        """
        """
        return self.spark
