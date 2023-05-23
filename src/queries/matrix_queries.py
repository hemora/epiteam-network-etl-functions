from enum import Enum

class MatrixQueries(Enum):
    """
    """
    SIZES_VM = lambda p : f"""
    WITH
    pre AS (
        SELECT *
            , MIN(cdmx_datetime) OVER() AS min_datetime
        FROM read_parquet('{p}')
        WHERE home_ageb IS NOT NULL
            AND cve_geo[:2] IN ('09', '13', '15') 
            AND home_ageb[:2] != '00'
    )

    SELECT home_ageb, COUNT(DISTINCT caid) AS size
    FROM pre
    GROUP BY 1
    ORDER BY 1 ASC
    """.strip()

    ALL_SIZES = lambda p : f"""
    WITH
    pre AS (
        SELECT *
            , MIN(cdmx_datetime) OVER() AS min_datetime
        FROM read_parquet('{p}')
        WHERE home_ageb IS NOT NULL
    )

    SELECT home_ageb, COUNT(DISTINCT caid) AS cardinalidad
    FROM pre
    GROUP BY 1
    ORDER BY 1 ASC
    """.strip()