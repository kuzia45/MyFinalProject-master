from sqlalchemy.orm import relationship
from database import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from table_post import Post
from table_user import User

class Feed(Base):
    __tablename__ = 'feed_action'
    action = Column(String)
    post_id = Column(Integer, ForeignKey('post.id'), primary_key=True)
    time = Column(TIMESTAMP)
    user_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
    user = relationship(User)
    post = relationship(Post)