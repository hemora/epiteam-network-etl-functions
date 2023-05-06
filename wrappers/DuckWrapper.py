import duckdb

class DuckWrapper:
    """
    """
    def __init__(self, name: str = "DuckSession") -> None:
        """
        """
        self.duck = duckdb.connect("./temp/file.db")

    def get_session(self):
        """
        """
        return self.duck