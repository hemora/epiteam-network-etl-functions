import time
from datetime import datetime, timedelta

class Stopwatch():
    def __init__(self) -> None:
        pass

    def __enter__(self):
        self.start = time.time()
        print(f"Started at {str(datetime.now())}")

    def __exit__(self, type, value, traceback):
        self.end = time.time()
        elapsed = self.end - self.start
        print(f"Ended at {str(datetime.now())}")
        print(f"Elapsed time: {str(timedelta(seconds=elapsed))}")