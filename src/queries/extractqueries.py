from enum import Enum

class ExtractQueries(Enum):
    """
    """
    PARQUET_READER = lambda y, m, d, p : f"""
    WITH
    raw_data AS (
        SELECT *
        FROM read_parquet('{p}/*')
    )

    , with_cdmx_datetime AS (
        SELECT *
            , TIMEZONE('America/Mexico_City', TO_TIMESTAMP(utc_timestamp)) AS cdmx_datetime
        FROM raw_data
    )

    SELECT utc_timestamp, cdmx_datetime, caid
        , latitude, longitude, horizontal_accuracy
    FROM with_cdmx_datetime
    WHERE STRFTIME(cdmx_datetime, '%Y-%m-%d') = '{y}-{m}-{d}'
        AND horizontal_accuracy >= 100
    """.strip()