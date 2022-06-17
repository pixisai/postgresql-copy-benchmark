from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    LargeBinary,
    select,
    case,
)
from random import randbytes, randint, random
from sqlalchemy.sql import Select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
import time

URI = "postgresql://postgres:1234@localhost:{port}/postgres"

Base = declarative_base()


class Metrics(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)

    metric_int1 = Column(Integer, default=lambda: randint(0, 10000))
    metric_int2 = Column(Integer, default=lambda: randint(-10000, 0))

    metric_float1 = Column(Float, default=random)
    metric_float2 = Column(Float, default=random)

    metric_string = Column(String, default="Hello World!")

    metric_binary = Column(LargeBinary, default=lambda: randbytes(100))


def _batch_insert(conn, result, model, mapping):  # Our default batch insert
    has_records = False
    while True:
        chunk = result.fetchmany(10000)
        if not chunk:
            break
        has_records = True
        conn.execute(insert(model), [mapping(st) for st in chunk])
    return has_records


def batch_insert(engine1: Engine, engine2: Engine, sel: Select) -> None:

    print("Run batch insert")
    start_time = time.time()

    res = engine1.execute(sel)
    _batch_insert(
        engine2,
        res,
        Metrics,
        lambda st: {col.key: getattr(st, col.key) for col in sel.selected_columns},
    )
    duration = time.time() - start_time

    print(f"Copied by batch insert by {duration} sec.")


def bin_copy(engine1: Engine, engine2: Engine, sel: Select) -> None:
    import psycopg

    conn1 = psycopg.connect(str(engine1.url))
    conn2 = psycopg.connect(str(engine2.url))

    copy_from_sel_stmt = str(sel.compile(compile_kwargs={"literal_binds": True}))
    copy_to_columns_stmt = ", ".join([col.key for col in sel.selected_columns])

    copy_from_stmt = "COPY (" + copy_from_sel_stmt + ") TO STDOUT (FORMAT BINARY)"
    copy_to_stmt = (
        "COPY metrics(" + copy_to_columns_stmt + ") FROM STDIN (FORMAT BINARY)"
    )

    print("Run binary copy")
    start_time = time.time()
    with conn1.cursor().copy(copy_from_stmt) as copy_from:
        with conn2.cursor().copy(copy_to_stmt) as copy_to:
            for data in copy_from:
                copy_to.write(data)

    conn2.commit()
    conn2.close()
    conn1.close()

    duration = time.time() - start_time

    print(f"Copied by COPY (BINARY) by {duration} sec.")


def prepare(engine1: Engine, engine2: Engine):
    Base.metadata.create_all(engine1)
    Base.metadata.drop_all(engine2)
    Base.metadata.create_all(engine2)

    res = engine1.execute(select(Metrics)).fetchone()
    if not res:
        bulk_obj_list = [Metrics() for _ in range(1_000_000)]
        engine1_session = Session(engine1)

        engine1_session.bulk_save_objects(bulk_obj_list)
        engine1_session.commit()

    res = engine2.execute(select(Metrics)).fetchone()
    if res:
        cleanup(engine2)


def cleanup(engine: Engine):
    engine.execute("TRUNCATE metrics")


def benchmark():
    engine1 = create_engine(URI.format(port=15432), echo=False)
    engine2 = create_engine(URI.format(port=15433), echo=False)

    prepare(engine1, engine2)

    stmts = [
        select(Metrics),
        select(Metrics).where(Metrics.id % 2 == 0),
        select(Metrics).order_by(Metrics.metric_int1),
        select(
            Metrics.id,
            Metrics.metric_int2.label("metric_int1"),
            case((Metrics.metric_int1 % 2 == 0, "even"), else_="odd").label(
                "metric_string"
            ),
        ),
    ]

    for stmt in stmts:
        print(f"Cur sql:\n{stmt.compile(compile_kwargs={'literal_binds': True})}")
        batch_insert(engine1, engine2, stmt)
        cleanup(engine2)
        bin_copy(engine1, engine2, stmt)

        input("Press any to next test...")
        cleanup(engine2)


if __name__ == "__main__":
    benchmark()
