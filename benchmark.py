"""
Script to benchmark the driver vs ORM
"""

from time import perf_counter
from typing import Callable
from concurrent.futures import ThreadPoolExecutor
import random

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
from psycopg_pool import ConnectionPool
from psycopg import sql
from tabulate import tabulate

DB_PATH = "postgresql://postgres:postgres@localhost:5431/postgres"

## SETUPS


class ORMBase(DeclarativeBase): ...


class table(ORMBase):
    __tablename__ = "bench_storage"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    short_val: Mapped[int] = mapped_column(nullable=False)
    long_val: Mapped[str] = mapped_column(nullable=True)


engine = create_engine(DB_PATH, pool_size=25)
SESSIONMAKER = sessionmaker(engine)
PG_POOLER = ConnectionPool(DB_PATH, max_size=25)


## HELPERS


def get_dummy_vals(
    runs: int = 100_000, return_tuples: bool = False
) -> list[dict | tuple]:
    """Factory to generaete sample values"""

    candidates = []
    for i in range(runs):
        data = {"short_val": i, "long_val": "value " * 100}

        if return_tuples:
            data = (data["short_val"], data["long_val"])

        candidates.append(data)

    return candidates


## MAIN RUNNERS


def run_sqlalchemy_select():
    """
    ### Description:
        - Executes a select operation using SQLAlchemy ORM.
        - Populates the database with test data before execution.
        - Uses threading for concurrent access.

    """

    populate_stmt = """
    INSERT INTO bench_storage (short_val,long_val)
    SELECT gs, 'value ' || gs
    FROM generate_series(1,1000000) as gs
    """
    with SESSIONMAKER() as s:
        s.execute(text(populate_stmt))
        s.commit()

    def func():
        with SESSIONMAKER() as session:
            result = (
                session.query(table).filter_by(id=random.randint(1, 1_000_000)).first()
            )
            session.commit()

    with ThreadPoolExecutor(max_workers=1000) as tpe:
        futures = [tpe.submit(func) for i in range(100_000)]


def run_psycopg_select():
    """
    ### Description:
        - Executes a select operation using Psycopg.
        - Populates the database with test data before execution.
        - Utilizes threading for handling concurrent requests.

    """

    populate_stmt = """
    INSERT INTO bench_storage (short_val,long_val)
    SELECT gs, 'value ' || gs
    FROM generate_series(1,1000000) as gs
    """
    with SESSIONMAKER() as s:
        s.execute(text(populate_stmt))
        s.commit()

    def func():
        stmt = "SELECT * FROM BENCH_STORAGE WHERE id = %s;"

        with PG_POOLER.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(stmt, (random.randint(1, 1_000_000),))
                result = cur.fetchone()
                conn.commit()

    with ThreadPoolExecutor(max_workers=1000) as tpe:
        futures = [tpe.submit(func) for i in range(100_000)]


def run_sqlalchemy_update():
    """
    ### Description:
        - Executes an update operation using SQLAlchemy ORM.
        - Prepares the database with necessary data before applying updates.
        - Uses a thread pool for concurrent execution of updates.

    """

    populate_stmt = """
    INSERT INTO bench_storage (short_val,long_val)
    SELECT gs, 'value ' || gs
    FROM generate_series(1,1000000) as gs
    """
    with SESSIONMAKER() as s:
        s.execute(text(populate_stmt))
        s.commit()

    def func():
        with SESSIONMAKER() as session:
            result = (
                session.query(table).filter_by(id=random.randint(1, 1_000_000)).first()
            )
            result.long_val = "New long value"
            session.commit()

    with ThreadPoolExecutor(max_workers=1000) as tpe:
        futures = [tpe.submit(func) for i in range(100_000)]


def run_psycopg_update():
    """
    ### Description:
        - Executes an update operation using Psycopg.
        - Prepares the database with test data prior to updates.
        - Implements concurrency through threading.

    """

    populate_stmt = """
    INSERT INTO bench_storage (short_val,long_val)
    SELECT gs, 'value ' || gs
    FROM generate_series(1,1000000) as gs
    """
    with SESSIONMAKER() as s:
        s.execute(text(populate_stmt))
        s.commit()

    def func():
        stmt = """
        UPDATE BENCH_STORAGE
        SET LONG_VALUE = 'New long value'
        WHERE id = %s;
        """

        with PG_POOLER.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(stmt, (random.randint(1, 1_000_000),))
                conn.commit()

    with ThreadPoolExecutor(max_workers=1000) as tpe:
        futures = [tpe.submit(func) for i in range(100_000)]


def run_sqlalchemy_add_all():
    """
    ### Description:
        - Executes a batch insert operation using SQLAlchemy ORM.
        - Retrieves dummy data to insert into the database in a single transaction.

    """

    with SESSIONMAKER() as session:
        session.add_all([table(**data) for data in get_dummy_vals()])
        session.commit()


def psycopg_executemany():
    """
    ### Description:
        - Executes a batch insert operation using Psycopg.
        - Retrieves dummy data for mass insertion in a single call.

    """

    stmt = """
    INSERT INTO BENCH_STORAGE(short_val,long_val)
    VALUES (%s,%s);
    """

    with PG_POOLER.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, get_dummy_vals(return_tuples=True))
            conn.commit()


def run_sqlalchemy_concurrent_add():
    """
    ### Description:
        - Executes a concurrent insert operation using SQLAlchemy ORM.
        - Uses threading to perform multiple concurrent insertions into the database.

    """

    def func():
        with SESSIONMAKER() as session:
            session.add(table(short_val=100, long_val="hello " * 100))
            session.commit()

    with ThreadPoolExecutor(max_workers=1000) as tpe:
        futures = [tpe.submit(func) for i in range(100_000)]


def psycopg_concurrent_add():
    """
    ### Description:
        - Executes a concurrent insert operation using Psycopg.
        - Utilizes threading to allow multiple insertions to occur simultaneously.

    """

    def func():
        stmt = """
        INSERT INTO BENCH_STORAGE(short_val,long_val)
        VALUES (%s,%s);
        """

        with PG_POOLER.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(stmt, (100, "hello " * 100))
                conn.commit()

    with ThreadPoolExecutor(max_workers=1000) as tpe:
        futures = [tpe.submit(func) for i in range(100_000)]


def benchmark_worker(function: Callable) -> float:
    """
    ### Description:
        - Benchmarks the time taken for different database operations.
        - Sets up database schema before running specified functions.
        - Captures and returns the duration of each operation.

    ### Args:
        - `function`: Callable
            The function representing the database operation to benchmark.

    ### Returns:
        - `time`: float
            The time taken (in seconds) to execute the benchmarked function.

    """

    ORMBase.metadata.drop_all(engine)
    ORMBase.metadata.create_all(engine)

    start = perf_counter()
    function()
    time = perf_counter() - start
    return time


def print_results(results: list[dict]):
    """
    ### Description:
        - Prints the benchmark results in a formatted table.
        - Calculates and displays the relative slowness of each operation.

    ### Args:
        - `results`: list[dict]
            List of dictionaries containing benchmark results to display.

    """

    for idx, method in enumerate(results):
        if idx % 2 == 0:
            continue
        prev = results[idx - 1]["duration"]
        method["slowness"] = str(round((method["duration"] / prev), 2)) + "x"

    data = [[x["name"], x["duration"], x["slowness"]] for x in results]
    display = tabulate(
        data,
        headers=["Method", "Duration", "Slowness"],
        tablefmt="pretty",
        stralign="left",
    )
    print(display)


def run_benchmark():
    """
    ### Description:
        - Runs a series of benchmarks comparing different database methods.
        - Organizes and executes the benchmark functions sequentially.
        - Outputs the results of each benchmark in a human-readable format.

    """

    functions = [
        {
            "name": "Psycopg Concurrent Select",
            "func": run_psycopg_select,
            "duration": 0,
            "slowness": "0",
        },
        {
            "name": "SQLAlchemy Concurrent Select",
            "func": run_sqlalchemy_select,
            "duration": 0,
            "slowness": "0",
        },
        {
            "name": "Psycopg Concurrent update",
            "func": run_psycopg_update,
            "duration": 0,
            "slowness": "0",
        },
        {
            "name": "SQLAlchemy Concurrent update",
            "func": run_sqlalchemy_update,
            "duration": 0,
            "slowness": "0",
        },
        {
            "name": "Psycopg Batch Add",
            "func": psycopg_executemany,
            "duration": 0,
            "slowness": "0",
        },
        {
            "name": "SQLAlchemy Batch Add",
            "func": run_sqlalchemy_add_all,
            "duration": 0,
            "slowness": "0",
        },
        {
            "name": "Psycopg Concurrent Add",
            "func": psycopg_concurrent_add,
            "duration": 0,
            "slowness": "0",
        },
        {
            "name": "SQLAlchemy Concurrent Add",
            "func": run_sqlalchemy_concurrent_add,
            "duration": 0,
            "slowness": "0",
        },
    ]

    for func in functions:
        func["duration"] = benchmark_worker(func["func"])

    print_results(functions)


if __name__ == "__main__":
    run_benchmark()
