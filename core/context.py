class Context:
    """
    """
    def __init__(self, year: str, month: str, day: str):
        self.__year = year
        self.__month = month
        self.__day = day

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
    
class ParquetContext(Context):
    """
    """
    def __init__(self, context: Context, parquet_path: str):
        self.__context = context
        super().__init__(context.year, context.month, context.day)
        self.__parquet_path = parquet_path

    #def __init__(self, year: str, month: str, day: str):
    #    super().__init__(year, month, day)
    
    @property
    def parquet_path(self):
        return self.__parquet_path
    @parquet_path.setter
    def parquet_path(self, value):
        self.__parquet_path = value
