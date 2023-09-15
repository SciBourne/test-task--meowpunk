from datetime import date
from pathlib import Path

from memory_profiler import profile
from loguru import logger

from error_reaper.reaper import ErrorReaper


@profile
def start():
    logger.info("Start ETL process")

    reaper = ErrorReaper(
        db_cheaters=Path("database/cheaters.db"),
        server_log=Path("dataset/server.csv"),
        client_log=Path("dataset/client.csv")
    )

    reaper.extract_records(target_date=date(2021, 1, 3))
    logger.success("Relevant records extracted")

    reaper.upload_records()
    logger.success("Records uploaded to database")
