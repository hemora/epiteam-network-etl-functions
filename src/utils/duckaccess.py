import duckdb

class DuckSession:
    """
    """
    def __init__(self, storage_path: str = None) -> None:
        if storage_path is None:
            self.__duck = duckdb.connect()
        else:
            self.__duck = duckdb.connect(storage_path)

    def __enter__(self):
        return self.__duck

    def __exit__(self, exception_type, exception_value, traceback):
        self.__duck.close()