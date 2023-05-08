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