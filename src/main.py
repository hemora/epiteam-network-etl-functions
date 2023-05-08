from extractors.DuckPoweredExtractors import ParquetExtractor
from core.context import ExtractContext


if __name__ == "__main__":

    ctxt = ExtractContext("2020", "01", "16")

    pe = ParquetExtractor()

    result = pe.handle(ctxt)

    print(type(result.payload))
    print(result.payload)