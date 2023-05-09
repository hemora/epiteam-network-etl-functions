from extractors.DuckPoweredExtractors import ParquetExtractor
from transforms.ntl_transforms import NTLPreparation, NTLWinners, NTLJoiner, NTLLocator
from core.context import ExtractContext, TransformContext, InteractionsContext, MatrixContext
from transforms.interactions_transforms import InteractionsCompute

from transforms.matrix_transforms import SizesMatrix, TotalEdgesMatrix, ObservedMatrix, TotalVsObservedMatrix, ProbAndSizesMatrix, NetworkGen

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

    ctxt = ExtractContext("2020", "01", "16")

    with T():

        pe = ParquetExtractor()

    result = pe.handle(ctxt)

    ctxt = TransformContext("2020", "01", "16")

    with T():

        ntl1 = NTLPreparation()
        ntl2 = NTLWinners()
        ntl3 = NTLJoiner()
        ntl4 = NTLLocator()

        ntl1.set_next(ntl2).set_next(ntl3).set_next(ntl4)
    
        result = ntl1.handle(ctxt)


    ctxt = InteractionsContext("2020", "01", "16")

    with T():

         ic1 = InteractionsCompute()

         result = ic1.handle(ctxt)

    ctxt = MatrixContext("2020", "01", "16")

    with T():

        mtx1 = SizesMatrix()
        mtx2 = TotalEdgesMatrix()
        mtx3 = ObservedMatrix()
        mtx4 = TotalVsObservedMatrix()
        mtx5 = ProbAndSizesMatrix()
        mtx6 = NetworkGen()

        mtx1.set_next(mtx2).set_next(mtx3) \
            .set_next(mtx4).set_next(mtx5).set_next(mtx6)

        result = mtx1.handle(ctxt)



    print(type(result.payload))
    print(result.payload)