from pathlib import Path

from sqlalchemy import (
    Engine,
    create_engine
)

import pandas as pd
from models import Error


class UpLoader:
    def __init__(self, db_path: Path):
        self.db_path = db_path

        self.engine: Engine = create_engine(
            url=f"sqlite:///{self.db_path}",
            echo=False
        )

        self.__refresh_result_table()

    def send_data(self,
                  table_name: str,
                  data_frame: pd.DataFrame) -> None:

        data_frame.to_sql(
            con=self.engine,
            name=Error.__tablename__,
            index=True,
            if_exists="replace",
            method="multi"
        )

    def __refresh_result_table(self) -> None:
        Error.metadata.drop_all(
            self.engine,
            tables=[Error.__table__]
        )

        Error.metadata.create_all(
            self.engine,
            tables=[Error.__table__]
        )
