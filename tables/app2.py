import os
from fastapi import FastAPI, Depends
from fastapi import HTTPException
from datetime import datetime
from pydantic import BaseModel
from loguru import logger
from psycopg2.extras import RealDictCursor
from sqlalchemy import desc, create_engine
from sqlalchemy.sql.functions import count
from sqlalchemy.orm import Session
from typing import List
from schema import UserGet, PostGet, FeedGet
import pandas as pd
from catboost import CatBoostClassifier


app = FastAPI()



def batch_load_sql(query: str) -> pd.DataFrame: #Загрузка таблицы батчами
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)



def get_model_path(path: str) -> str: #Внутренняя проверка
    if os.environ.get("IS_LMS") == "1": 
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models(): #Функция для загрузки модели
    model = CatBoostClassifier()
    model_path = get_model_path("C:/Users/Konstantin/Downloads/catboost_modelDL.cbm")
    model.load_model(model_path)
    return model


def load_features():
    logger.info('loading liked posts')
    liked_posts_query = """SELECT distinct post_id, user_id
                        FROM public.feed_data
                        WHERE action = 'like'"""
    liked_posts = batch_load_sql(liked_posts_query)

    logger.info('loading post features')
    post_features = pd.read_sql("SELECT * FROM mfkky2", con = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml")
    
    logger.info('loading user features')
    user_features = pd.read_sql("SELECT * FROM user_data", con = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml")
    
    return [liked_posts, post_features, user_features]

logger.info('loading model')
model = load_models()
logger.info('loading features')
features = load_features()



def get_recommended_posts(id: int, time:datetime, limit:int = 5):
    logger.info(f'user_id:{id}')
    user_features = features[2].loc[features[2].user_id == id]
    user_features = user_features.drop(['user_id'], axis=1)

    post_features = features[1].drop(['index'], axis=1)
    content = pd.read_sql("SELECT * FROM public.post_text_df", con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml")

    logger.info('zipping')
    zipping = dict(zip(user_features.columns, user_features.values[0]))
    logger.info('merge')
    common_features = post_features.assign(**zipping)
    common_features = common_features.set_index('post_id')
    logger.info('add time')
    common_features['hour'] = time.hour
    common_features['month'] = time.month
    common_features = common_features[['hour', 'month', 'gender', 'age', 'country', 'city', 'exp_group', 'os',
        'source', 'topic', 'TextCluster', 'DistanceToCluster_0',
        'DistanceToCluster_1', 'DistanceToCluster_2', 'DistanceToCluster_3',
        'DistanceToCluster_4', 'DistanceToCluster_5', 'DistanceToCluster_6',
        'DistanceToCluster_7', 'DistanceToCluster_8', 'DistanceToCluster_9',
        'DistanceToCluster_10', 'DistanceToCluster_11', 'DistanceToCluster_12',
        'DistanceToCluster_13', 'DistanceToCluster_14']]
    print (common_features)
    logger.info('predicting')
    predicts = model.predict_proba(common_features)[:, 1]
    common_features['predicts']=predicts
    logger.info('deleting liked posts')
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    filtred = common_features[~common_features.index.isin(liked_posts)]

    logger.info('recommendation')
    recommended_posts = filtred.sort_values('predicts')[-limit:].index

    return [PostGet(**{'id': i, 'text': content[content.post_id == i].text.values[0], 
                       'topic': content[content.post_id == i].topic.values[0]}) for i in recommended_posts]
    


@app.get('/post/recommendations/', response_model=List[PostGet])
def recommended_posts(id:int, time: datetime, limit:int = 10):
   return  get_recommended_posts(id, time, limit)