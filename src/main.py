from extractors.DuckPoweredExtractors import ParquetExtractor
from transforms.ntl_transforms import NTLPreparation, NTLWinners, NTLJoiner, NTLLocator
from core.context import ExtractContext, TransformContext

import time
from datetime import timedelta

class T():
    def __init__(self) -> None:
        pass

    def __enter__(self):
        self.start = time.time()
        print(f"Started at {str(self.start)}")

    def __exit__(self, type, value, traceback):
        self.end = time.time()
        elapsed = self.end - self.start
        print(f"Ended at {str(self.start)}")
        print(f"Elapsed time: {str(timedelta(seconds=elapsed))}")


if __name__ == "__main__":

    #ctxt = ExtractContext("2020", "01", "16")

    #pe = ParquetExtractor()

    ctxt = TransformContext("2020", "01", "16")

    with T():

        ntl1 = NTLPreparation()
        ntl2 = NTLWinners()
        ntl3 = NTLJoiner()
        ntl4 = NTLLocator()

        ntl1.set_next(ntl2).set_next(ntl3).set_next(ntl4)

        result = ntl1.handle(ctxt)

    print(type(result.payload))
    print(result.payload)