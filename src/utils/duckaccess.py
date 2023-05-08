import duckdb

class DuckSession:
    """
    """
    def __init__(self) -> None:
        self.__duck = duckdb.connect()

    def __enter__(self):
        return self.__duck

    def __exit__(self, exception_type, exception_value, traceback):
        self.__duck.close()