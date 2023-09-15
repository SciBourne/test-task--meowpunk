from uuid import UUID
from datetime import datetime

from sqlalchemy import Integer, DateTime, JSON, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from models import Base


class Error(Base):
    __tablename__ = "errors"

    error_id: Mapped[UUID] = mapped_column(
        Uuid,
        primary_key=True
    )

    player_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )

    event_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False
    )

    json_server: Mapped[dict[str, str]] = mapped_column(
        JSON,
        nullable=False
    )

    json_client: Mapped[dict[str, str]] = mapped_column(
        JSON,
        nullable=False
    )

    def __init__(self,
                 timestamp: datetime,
                 json_client: dict[str, str],
                 json_server: dict[str, str],
                 error_id: UUID,
                 player_id: int,
                 event_id: int | None = None):

        self.timestamp = timestamp
        self.error_id = error_id
        self.player_id = player_id
        self.event_id = event_id
        self.json_client = json_client
        self.json_server = json_server

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            "("
            f"timestamp={self.timestamp}, "
            f"error_id={self.error_id}, "
            f"player_id={self.player_id}, "
            f"event_id={self.event_id}, "
            f"json_client={self.json_client}, "
            f"json_server={self.json_server}"
            ")"
        )
