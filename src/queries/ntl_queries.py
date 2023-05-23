from enum import Enum

class NTLQueries(Enum):
    """
    """

    UNIQUE_CAIDS = lambda p : f"""
    SELECT DISTINCT caid
    FROM read_parquet('{p}')
    """.strip()

    WINNERS = lambda v : f"""
    WITH
    pre AS (
        SELECT caid, h3index_12
            , STRFTIME(cdmx_datetime, '%Y-%m-%d') AS cdmx_date
        FROM {v}
    )

    , pings_per_day AS (
        SELECT caid, h3index_12, cdmx_date
            , COUNT(*) AS pings_per_day
        FROM pre
        GROUP BY 1, 2, 3
    )

    , with_total_pings AS (
        SELECT *
            , (SUM(pings_per_day) OVER (PARTITION BY caid))::INTEGER AS total_pings
        FROM pings_per_day
    )

    , scores AS (
        SELECT caid, h3index_12
            , SUM(pings_per_day) AS score
        FROM with_total_pings
        WHERE total_pings >= 10 AND pings_per_day >= 5
        GROUP BY 1, 2
    )

    SELECT caid, h3index_12
    FROM (
        SELECT *
            , ROW_NUMBER() OVER (PARTITION BY caid ORDER BY score DESC) AS rank
        FROM scores
    )
    WHERE rank = 1
    """.strip()

    JOIN = lambda p : f"""
    WITH
    raw_data AS (
        SELECT *
        FROM read_parquet('{p}')
    )

    SELECT a.utc_timestamp, a.cdmx_datetime, a.caid
        , a.latitude, a.longitude, a.horizontal_accuracy
        , IF(b.h3index_12 IS NULL, '000000000000000', b.h3index_12) AS home_h3index_12
    FROM 
        raw_data AS a
        LEFT JOIN
        home_ageb_catalog AS b
        ON a.caid = b.caid
    """.strip()

    SELECT_IF_EXISTS = lambda p : f"""
    WITH
    pre AS (
        SELECT *
        FROM read_parquet('{p}')
        WHERE home_h3index_12 != '000000000000000'
    )

    SELECT *
    FROM pre
    """.strip()

    SELECT_NOT_EXISTS = lambda p : f"""
    WITH
    pre AS (
        SELECT *
        FROM read_parquet('{p}')
        WHERE home_h3index_12 = '000000000000000'
    )

    SELECT *
    FROM pre
    """.strip()

    EXTRACT_IN_DATE_RANGE = lambda s, p : f"""
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
    WHERE STRFTIME(cdmx_datetime, '%Y-%m-%d') in {s}
        AND horizontal_accuracy >= 100
        AND (DATEPART('hour', cdmx_datetime) >= 22 OR DATEPART('hour', cdmx_datetime) < 6)
    """.strip()