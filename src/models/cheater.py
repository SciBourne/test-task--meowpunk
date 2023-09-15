from datetime import datetime

from sqlalchemy import Integer, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from models import Base


class Cheater(Base):
    __tablename__ = "cheaters"

    player_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True
    )

    ban_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True
    )

    def __init__(self, player_id: int, ban_time: datetime):
        self.player_id = player_id
        self.ban_time = ban_time

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            "("
            f"player_id={self.player_id}, "
            f"ban_time={self.ban_time}"
            ")"
        )
