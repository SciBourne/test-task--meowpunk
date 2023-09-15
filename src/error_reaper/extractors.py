import os
import psutil

from abc import ABC, abstractmethod
from collections.abc import Iterator

from datetime import date, timedelta
from pathlib import Path
from enum import Enum

import pandas as pd

from sqlalchemy import (
    Engine,
    create_engine,
    Select,
    select,
    func,
    text,
    between
)

from models import Base, Cheater


class SourceType(str, Enum):
    sqlite = "sqlite"
    csv = "csv"


class DataExtractor(ABC):
    @abstractmethod
    def get_chunks(self,
                   index_col: str,
                   target_date: date) -> Iterator[pd.DataFrame]:
        ...


class FileExtractor(DataExtractor):
    def __init__(self, path: Path, column_dtypes: dict[str, type]):
        self.path = path
        self.column_dtypes = column_dtypes

    @property
    def row_count(self) -> int:
        with open(self.path, 'r') as file:
            return sum(
                chunk.count('\n') for chunk in iter(
                    lambda: file.read(1 << 8), ''
                )
            )

    @property
    def chunksize(self) -> int:
        return int(
            (psutil.virtual_memory().free * 0.75) /
            (os.path.getsize(self.path) / self.row_count)
        )


class CSVExtractor(FileExtractor):
    def get_chunks(self,
                   index_col: str,
                   target_date: date) -> Iterator[pd.DataFrame]:

        with pd.read_csv(self.path,
                         dtype=self.column_dtypes,
                         index_col=index_col,
                         chunksize=self.chunksize) as data:

            for chunk in data:
                chunk.timestamp = pd.to_datetime(
                    chunk.timestamp,
                    unit='s'
                )

                yield chunk.loc[
                    chunk["timestamp"].between(
                        pd.Timestamp(target_date),

                        pd.Timestamp(target_date) + pd.Timedelta(
                            hours=23,
                            minutes=59,
                            seconds=59
                        )
                    )
                ]


class SQLExtractor(DataExtractor):
    def __init__(self,
                 path: Path,
                 table: Base,
                 column_dtypes: dict[str, type]):

        self.path = path
        self.table = table
        self.column_dtypes = column_dtypes

    @property
    @abstractmethod
    def engine(self) -> Engine:
        ...

    @property
    def chunksize(self) -> int:
        with self.engine.connect() as connection:
            row_count: int = connection.scalar(
                select(func.count(self.table.player_id))
            )

            data_size: int = connection.scalar(
                text(
                    "SELECT sum(pgsize) FROM dbstat"
                    f" WHERE name = '{self.table.__tablename__}'"
                )
            )

        return int(
            (psutil.virtual_memory().free * 0.75) /
            (data_size / row_count)
        )


class SQLiteExtractor(SQLExtractor):
    @property
    def engine(self) -> Engine:
        return create_engine(
            url=f"sqlite:///{self.path}",
            echo=False
        )

    def get_chunks(self,
                   index_col: str,
                   target_date: date) -> Iterator[pd.DataFrame]:

        query: Select = (
            select(Cheater.player_id,
                   Cheater.ban_time)
            .where(
                between(
                    Cheater.ban_time,

                    target_date - timedelta(
                        days=1
                    ),

                    target_date + timedelta(
                        hours=23,
                        minutes=59,
                        seconds=59
                    )
                )
            )
        )

        with self.engine.connect() as connection:
            return pd.read_sql_query(
                sql=query,
                con=connection,
                index_col=index_col,
                chunksize=self.chunksize
            )


def create_extractor(source_type: SourceType,
                     column_dtypes: dict[str, type],
                     path: Path,
                     table: Base | None = None) -> DataExtractor:

    match source_type:
        case SourceType.csv:
            return CSVExtractor(
                path=path,
                column_dtypes=column_dtypes
            )

        case SourceType.sqlite:
            return SQLiteExtractor(
                path=path,
                table=table,
                column_dtypes=column_dtypes
            )

        case _:
            raise NotImplementedError
