from enum import Enum

class InteractionQueries(Enum):
    """
    """

    INTERACTIONS = lambda p: f"""
    WITH
    pre AS (
        SELECT *
            , MIN(cdmx_datetime) OVER() AS min_datetime
        FROM read_parquet('{p}')
    )

    , pings_base AS (
        SELECT *
            , TIME_BUCKET(INTERVAL '600 seconds', cdmx_datetime::TIMESTAMP, min_datetime::TIMESTAMP) AS tw
        FROM pre
    )
    
    SELECT  DISTINCT a.caid AS a_caid, a.home_ageb AS a_home_ageb
        , b.caid AS b_caid, b.home_ageb AS b_home_ageb
    FROM pings_base AS a
        INNER JOIN
        pings_base AS b
        ON a.h3index_15 = b.h3index_15
            AND a.tw = b.tw
        WHERE a.caid != b.caid 
    """.strip()

    INTERACTIONS_IN_VM = lambda p : f"""
    WITH
    pre AS (
        SELECT *
            , MIN(cdmx_datetime) OVER() AS min_datetime
        FROM read_parquet('{p}')
        WHERE home_ageb IS NOT NULL 
            AND cve_geo[:2] IN ('09', '13', '15') 
            AND home_ageb[:2] != '00'
    )

    , pings_base AS (
        SELECT *
            , TIME_BUCKET(INTERVAL '600 seconds', cdmx_datetime::TIMESTAMP, min_datetime::TIMESTAMP) AS tw
        FROM pre
    )
    
    SELECT  DISTINCT a.caid AS a_caid, a.home_ageb AS a_home_ageb
        , b.caid AS b_caid, b.home_ageb AS b_home_ageb
    FROM pings_base AS a
        INNER JOIN
        pings_base AS b
        ON a.h3index_15 = b.h3index_15
            AND a.tw = b.tw
        WHERE a.caid != b.caid
    """.strip()

    ALL_INTERACTIONS = lambda p : f"""
    WITH
    pre AS (
        SELECT *
            , MIN(cdmx_datetime) OVER() AS min_datetime
        FROM read_parquet('{p}')
        WHERE home_ageb IS NOT NULL
    )

    , pings_base AS (
        SELECT *
            , TIME_BUCKET(INTERVAL '600 seconds', cdmx_datetime::TIMESTAMP, min_datetime::TIMESTAMP) AS tw
        FROM pre
    )
    
    SELECT  DISTINCT a.caid AS a_caid, a.home_ageb AS a_home_ageb
        , b.caid AS b_caid, b.home_ageb AS b_home_ageb
    FROM pings_base AS a
        INNER JOIN
        pings_base AS b
        ON a.h3index_15 = b.h3index_15
            AND a.tw = b.tw
        WHERE a.caid != b.caid
    """.strip()