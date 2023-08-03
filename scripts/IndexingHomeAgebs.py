import sys
sys.path.append("/home/hmora/network-gen-pipeline/src")

import utils.DateUtils as du
import utils.path_utils as pu
from utils.stopwatch import Stopwatch
from utils.duckaccess import DuckSession
from data_classes.date_container import DateContext

import duckdb

import os

from dotenv import load_dotenv
import os
load_dotenv()

import multiprocessing as mp

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

import time

import numpy as np

import concurrent.futures

import click

def get_last_15_days_paths(date_ctxt: DateContext) -> list[str]:
    """
    """
    year, month, day = date_ctxt.as_tuple()

    _, last_15_days = du.get_last_dates(year, month, day, 15)

    last_15_paths = \
        [f"{os.environ['WAREHOUSE_PATH']}/home_ageb_repository/{d.year}_{d.month}_{d.day}" \
            for d in last_15_days]
    
    return last_15_paths

def get_last_15_datasets(paths: list[str]) -> list[pa.Table]:
    """
    """
    get_last_15_datasets = \
        [ds.dataset(p).to_table().combine_chunks() for p in paths]

    return get_last_15_datasets

def merge_datasets(datasets: list[pa.Table]) -> pa.Table:
    """
    """
    merged_ds = ds.dataset(datasets).to_table().combine_chunks()

    return merged_ds

def get_home_ageb_catalog(arrow_table: pa.Table) -> pa.Table:
    """
    """
    with DuckSession() as duck:

        global_winners = duck.sql("""
        WITH
        scored_local_winners AS (
            SELECT caid, home_ageb
                , COUNT(*) AS score
            FROM arrow_table
            GROUP BY 1, 2
        )

        , ranked AS (
            SELECT *
                , ROW_NUMBER() OVER(PARTITION BY caid ORDER BY len(home_ageb) DESC, score DESC) AS rank
            FROM scored_local_winners
        )

        , best_ranked AS (
            SELECT *
            FROM ranked
            WHERE rank = 1
        )

        SELECT *
        FROM best_ranked
        """).arrow().combine_chunks()

    return global_winners

def single_date_indexing(year: str, month: str, day: str, write_path: str) -> pa.Table:
    """
    """
    last_15_paths = get_last_15_days_paths(DateContext(year, month, day))
    # print(last_15_paths)

    last_15_datasets = get_last_15_datasets(last_15_paths)
    # print(last_15_datasets)

    merged_ds = merge_datasets(last_15_datasets)
    # print(merged_ds)

    home_ageb_catalog = get_home_ageb_catalog(merged_ds)

    df = \
        ds.dataset(f"{os.environ['WAREHOUSE_PATH']}/fact_pings_base/{year}_{month}_{day}_pings") \
        .to_table().combine_chunks()
    
    with DuckSession() as duck:
        joined_with_catalog = duck.sql(f"""
        WITH
        zm_catalog AS (
            SELECT * EXCLUDE (geometry)
            FROM read_parquet('{os.environ['ZM_CATALOG']}')
        )

        , indexed_pings AS (
            SELECT 
                a.*
                , IF(b.home_ageb IS NULL
                    , '0000000000000', b.home_ageb) AS home_ageb
                , YEAR(cdmx_datetime) AS year
                , RIGHT(CONCAT('0', MONTH(cdmx_datetime)), 2) AS month
                , RIGHT(CONCAT('0', DAY(cdmx_datetime)), 2) AS day
            FROM
                df AS a
                LEFT JOIN
                home_ageb_catalog AS b
                ON a.caid = b.caid
        )

        SELECT a.*
            , b.cve_zm
        FROM 
            indexed_pings AS a
            LEFT JOIN
            zm_catalog AS b
            ON a.cve_mun = b.cve_geo
        """).arrow().combine_chunks()


    pq.write_to_dataset(joined_with_catalog
                        , root_path=write_path
                        , partition_cols=["year", "month", "day", "cve_zm"])

    print(duckdb.arrow(joined_with_catalog))

    return joined_with_catalog

def driver(t: tuple):
    """
    """
    year, month, day, write_path = t
    return single_date_indexing(year, month, day, write_path)


###########################################################################################3
@click.command(help="""
               Tool to assign a home_ageb for every ping in
               the provided date/period paramenter
               """)
@click.option("--date", default=None
              , help="""
              A string date like %Y-%m-%d that represents the set of pings
              to assign a home_ageb field based on last 15 day's data.
              """)
@click.option("--period", default=None
              , help="""
              A string date like %Y-%m that represents the set of pings for
              the specified period to assign a home_ageb field based on the
              last 15 day's data.
              """)
@click.option('-t', "--target"
              , default=f"{os.environ['WAREHOUSE_PATH']}/pings_with_home_ageb"
              , help="""
              The target directory in which the data would be written.
              """)
def main(date: str, period: str, target: str) -> None:
    """
    """
    if date is not None and period is not None:
        click.echo("Provide only --date or --period")
        sys.exit()


    if date is not None:

        year, month, day = date.split('-')
        single_date_indexing(year, month, day, target)
    
    elif period is not None:

        year, month = period.split('-')
        start, end = du.first_and_last_days(year, month)

        batches = \
            [(year, month, str(i).zfill(2), target) \
             for i in range(start, end+1)]
        
        print(batches)
        
        # with mp.Pool(3) as p:
        #     p.map(driver, batches) 

        for b in batches:
            driver(b)


if __name__ == "__main__":

    with Stopwatch():
        main()
