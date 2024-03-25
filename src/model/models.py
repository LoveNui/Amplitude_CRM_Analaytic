from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from .database import Base


class User(Base):
    __tablename__ = "users"
    amplitude_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=True)
    total_amont = Column(Integer, nullable=False)
    purchase = Column(Integer, nullable=False)
    video = Column(Integer, nullable=False)
    event = Column(Integer, nullable=False)
    session = Column(Integer, nullable=False)
    first_session = Column(Integer, nullable=True)
    last_session = Column(Integer, nullable=True)
    