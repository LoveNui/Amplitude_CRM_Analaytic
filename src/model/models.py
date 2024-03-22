from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from .database import Base


class User(Base):
    __tablename__ = "users"

    user_id = Column(String, primary_key=True, index=True)
    total_amont = Column(Integer, nullable=False)
    purchase = Column(Integer, nullable=False)
    video = Column(Integer, nullable=False)
    event = Column(Integer, nullable=False)
    session = Column(Integer, nullable=False)
    last_session = Column(Integer, nullable=True)