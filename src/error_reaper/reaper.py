from collections.abc import Iterator
from pathlib import Path
from datetime import date

import numpy as np
import pandas as pd
from tqdm import tqdm

from error_reaper.extractors import (
    DataExtractor,
    SourceType,
    create_extractor
)

from error_reaper.uploader import UpLoader
from models import Cheater


class ErrorReaper:
    CHEATER_EXTRACTOR: DataExtractor
    SERVER_EXTRACTOR: DataExtractor
    CLIENT_EXTRACTOR: DataExtractor

    UPLOADER: UpLoader

    cheater_frames: Iterator[pd.DataFrame]
    server_frames: Iterator[pd.DataFrame]
    client_frames: Iterator[pd.DataFrame]

    relevant_errors: pd.DataFrame

    def __init__(self,
                 db_cheaters: Path,
                 server_log: Path,
                 client_log: Path):

        self.CHEATER_EXTRACTOR = create_extractor(
            source_type=SourceType.sqlite,
            path=db_cheaters,
            table=Cheater,

            column_dtypes={
                "player_id": np.uint32,
                "ban_time": np.str_
            }
        )

        self.SERVER_EXTRACTOR = create_extractor(
            source_type=SourceType.csv,
            path=server_log,

            column_dtypes={
                "error_id": np.str_,
                "event_id": np.uint32,
                "timestamp": np.uint32,
                "description": np.str_
            }
        )

        self.CLIENT_EXTRACTOR = create_extractor(
            source_type=SourceType.csv,
            path=client_log,

            column_dtypes={
                "error_id": np.str_,
                "player_id": np.uint32,
                "timestamp": np.uint32,
                "description": np.str_
            }
        )

        self.UPLOADER = UpLoader(
            db_path=db_cheaters
        )

    def extract_records(self, target_date: date) -> None:
        self.__init_data_chunks(target_date)

        cheaters: pd.DataFrame = self.__get_cheaters()
        error_log: pd.DataFrame = self.__get_joined_logs()

        relevant_errors = error_log.join(
            cheaters,
            on="player_id",
            how="left"
        )

        relevant_errors = relevant_errors.loc[
            (relevant_errors["timestamp"] < relevant_errors["ban_time"]) |
            (relevant_errors["ban_time"].isna())
        ]

        self.relevant_errors = relevant_errors[
            ["player_id",
             "event_id",
             "timestamp",
             "json_server",
             "json_client"]
        ]

    def upload_records(self) -> None:
        tqdm.write("Upload result DataFrame")

        with tqdm(desc="PROGRESS", colour="green", ncols=80) as fake_progress:
            self.UPLOADER.send_data(
                data_frame=self.relevant_errors
            )

            fake_progress.update()

    def __init_data_chunks(self, target_date: date) -> None:
        self.cheater_frames = self.CHEATER_EXTRACTOR.get_chunks(
            index_col="player_id",
            target_date=target_date
        )

        self.server_frames = self.SERVER_EXTRACTOR.get_chunks(
            index_col="error_id",
            target_date=target_date
        )

        self.client_frames = self.CLIENT_EXTRACTOR.get_chunks(
            index_col="error_id",
            target_date=target_date
        )

    def __get_cheaters(self) -> pd.DataFrame:
        cheaters: list[pd.DataFrame] = []
        tqdm.write("Extract cheaters:")

        for cheater_frame in tqdm(self.cheater_frames,
                                  desc="PROGRESS",
                                  colour="green",
                                  ncols=80):

            cheaters.append(cheater_frame)
        return pd.concat(cheaters)

    def __get_joined_logs(self) -> pd.DataFrame:
        error_log: list[pd.DataFrame] = []
        tqdm.write("Extract joined error log:")

        for server_frame in tqdm(self.server_frames,
                                 desc="PROGRESS",
                                 colour="green",
                                 ncols=80):

            server_frame = server_frame.rename(
                columns={"description": "json_server"}
            )

            for client_frame in self.client_frames:
                client_frame = client_frame.rename(
                    columns={"description": "json_client"}
                )

                server_frame = server_frame.join(
                    client_frame[["player_id", "json_client"]],
                    on="error_id",
                    how="inner"
                )

            error_log.append(server_frame)
        return pd.concat(error_log)
