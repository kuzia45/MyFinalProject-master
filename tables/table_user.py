from database import Base, SessionLocal
from sqlalchemy import Column, Integer, String, desc
from sqlalchemy.sql.functions import count


class User(Base):
    __tablename__ = 'user'
    age = Column(Integer)
    city = Column(String)
    country = Column(String)
    exp_group = Column(Integer)
    gender = Column(Integer)
    id = Column(Integer, primary_key=True)
    os = Column(String)
    source = Column(String)


if __name__ == '__main__':
    session = SessionLocal()
    result = []
    for item in session.query(User.country, User.os, count('*')).filter(User.exp_group == 3).group_by(User.country, User.os).having(count('*') > 100).order_by(desc(count('*'))).all():
        result.append(item)
    print(result)
