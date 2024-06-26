from sqlalchemy.orm import Session, sessionmaker
from .models import User, Base
from .database import SessionLocal, engine
from typing import Optional

Base.metadata.create_all(bind=engine)
db = SessionLocal()

def create_user(amplitude_id: int, user_id:str, total_amont: int, purchase: int, video: int, event: int, session: int, first_session:Optional[int] = None, last_session:Optional[int] = None):
    db_user = User(amplitude_id=amplitude_id, user_id=user_id, total_amont = total_amont, purchase = purchase, video = video, event = event, session = session,first_session = first_session, last_session = last_session)
    try:
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
    except:
        return False
    return db_user

# Define the default chain updating function
def update_user(amplitude_id:int, user_id:str, total_amont: int, purchase: int, video: int, event: int, session: int, first_session:Optional[int] = None, last_session:Optional[int] = None):
    user = db.query(User).filter(User.amplitude_id == amplitude_id).update(
        {
            "user_id" : user_id,
            "total_amont" : total_amont,
            "purchase" : purchase,
            "video" : video,
            "event" : event,
            "session" : session,
            "first_session" : first_session,
            "last_session" : last_session
        })
    try:
        db.commit()
    except:
        return False
    return user

# Define function to get user with id 
def get_user_by_id(amplitude_id:int):
    user = db.query(User).filter(User.amplitude_id == amplitude_id).first()
    if not user:
        return False
    return user

def get_user_by_user_id(user_id:int):
    user = db.query(User).filter(User.user_id == user_id).first()
    if not user:
        return False
    return user