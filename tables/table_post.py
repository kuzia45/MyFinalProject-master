from database import Base, SessionLocal
from sqlalchemy import Column, Integer, String, desc


class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True, name='id')
    text = Column(String)
    topic = Column(String)



if __name__ == '__main__':
    session = SessionLocal()
    result = list()
    for item in session.query(Post).filter(Post.topic == 'business').order_by(desc(Post.id)).limit(10).all():
        result.append(item.id)
    print (result)