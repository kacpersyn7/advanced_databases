#%% Imports
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, DateTime, MetaData, ForeignKey, Table, Sequence, and_
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import numpy as np
from sqlalchemy.sql.schema import PrimaryKeyConstraint
# base.metadata.drop_all(engine)
#%% Run postgresql
db_string = 'postgresql://postgres:projekt@localhost:5432/twitter_analyzer'
engine = create_engine(db_string)
base = declarative_base()        
meta = MetaData()

# Association table MUser - Twitt
association_muser_twitt = Table('association_muser_twitt', base.metadata,
    Column('muser_id', Integer, ForeignKey('muser.id')),
    Column('twitt_id', Integer, ForeignKey('twitt.id')),
    PrimaryKeyConstraint('muser_id', 'twitt_id')
)

# Association table Hashtag - Twitt
association_hashtag_twitt = Table('association_hashtag_twitt', base.metadata,
    Column('hashtag_id', Integer, ForeignKey('hashtag.id')),
    Column('twitt_id', Integer, ForeignKey('twitt.id')),
    PrimaryKeyConstraint('hashtag_id', 'twitt_id')
)

# Tables definitions
class Author(base):
    __tablename__ = 'author'
    id = Column(Integer, Sequence('author_id'), primary_key=True)
    # id_str = Column('id_str', String, unique=True)
    author_name = Column('author_name', String)
    twitt = relationship("Twitt", back_populates="author")
    
    def __init__(self, author_name):
        # self.id_str = id_str
        self.author_name = author_name

class Twitt(base):
    __tablename__ = 'twitt'
    id = Column(Integer, Sequence('twitt_id'), primary_key=True)
    author_id = Column(Integer, ForeignKey('author.id'))
    author = relationship('Author', back_populates="twitt")
    
    muser = relationship("Muser", secondary=association_muser_twitt)
    hashtag = relationship("Hashtag", secondary=association_hashtag_twitt)
    
    text = Column('text', String)
    twitt_real_id = Column('twitt_real_id', String, unique=True)
    
    def __init__(self, twitt_real_id, text):
        self.twitt_real_id = twitt_real_id
        self.text = text
    
class Muser(base):
    __tablename__ = 'muser'
    id = Column(Integer, Sequence('muser_id'), primary_key=True)
    mentioned_user_name = Column('mentioned_user_name', String)
    
    def __init__(self, mentioned_user_name):
        self.mentioned_user_name = mentioned_user_name

class Hashtag(base):
    __tablename__ = 'hashtag'
    id = Column(Integer, Sequence('hashtag_id'), primary_key=True)
    hashtag_name = Column('hashtag_name', String)

    def __init__(self, hashtag_name):
        self.hashtag_name = hashtag_name

#%% database connection
conn = engine.connect()
base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

#%% read data
import pandas as pd
tweets_df = pd.read_csv('data_koronawirus.csv')
tweets_df['id'] = tweets_df['id'].astype(str)
tweets_df = tweets_df.fillna('')
#%% AUTHOR TABLE
authors_list = pd.DataFrame(tweets_df['author_name'].unique(), columns=['author'])
authors_list = authors_list.rename(columns = {'author':'author_name'})
authors_list.index.name = 'id'
#%% TWITT TABLE
twitt_list = tweets_df[['id', 'text', 'author_name']].drop_duplicates()
twitt_list = twitt_list.rename(columns = {'author_name':'author_id', 'id': 'twitt_real_id'}).reset_index().drop(columns = ['index'])
twitt_list['author_id'] = twitt_list['author_id'].map(lambda x:  authors_list[authors_list['author_name'] == x].index.values.astype(int)[0])
twitt_list.index.name = 'id'
#%% MUSER TABLE
muser_list = pd.DataFrame(tweets_df['user_mentions'].unique(), columns=['user_mentions'])
muser_list = muser_list.rename(columns = {'user_mentions':'mentioned_user_name'})
muser_list.index.name = 'id'

#%% HASHTAG TABLE
hashtag_list = pd.DataFrame(tweets_df['hashtags'].unique(), columns=['hashtags'])
hashtag_list = hashtag_list.rename(columns = {'hashtags':'hashtag_name'})
hashtag_list.index.name = 'id'
#%% TWITT-MSUER
twitt_muser_list = tweets_df[['id', 'user_mentions']].drop_duplicates()
twitt_muser_list = twitt_muser_list.rename(columns = {'id':'twitt_id', 'user_mentions':'muser_id'}).reset_index().drop(columns=['index'])
twitt_muser_list['twitt_id'] = twitt_muser_list['twitt_id'].map(lambda x:  twitt_list[twitt_list['twitt_real_id'] == x].index.values.astype(int)[0])
twitt_muser_list['muser_id'] = twitt_muser_list['muser_id'].map(lambda x:  muser_list[muser_list['mentioned_user_name'] == x].index.values.astype(int)[0])
# twitt_muser_list.index.name = 'id'
#%% TWITT-HASHTAG
twitt_hashtag_list = tweets_df[['id', 'hashtags']].drop_duplicates()
twitt_hashtag_list = twitt_hashtag_list.rename(columns = {'id':'twitt_id', 'hashtags':'hashtag_id'}).reset_index().drop(columns=['index'])
twitt_hashtag_list['twitt_id'] = twitt_hashtag_list['twitt_id'].map(lambda x:  twitt_list[twitt_list['twitt_real_id'] == x].index.values.astype(int)[0])
twitt_hashtag_list['hashtag_id'] = twitt_hashtag_list['hashtag_id'].map(lambda x:  hashtag_list[hashtag_list['hashtag_name'] == x].index.values.astype(int)[0])
# twitt_hashtag_list.index.name = 'id'
#%%
muser_list.to_sql('muser',engine, if_exists='append')
hashtag_list.to_sql('hashtag',engine, if_exists='append')
authors_list.to_sql('author',engine, if_exists='append')
# %%
twitt_list.to_sql('twitt',engine, if_exists='append')
#%%
twitt_muser_list.set_index(['muser_id']).to_sql('association_muser_twitt',engine, if_exists='append')
twitt_hashtag_list.set_index(['hashtag_id']).to_sql('association_hashtag_twitt',engine, if_exists='append')
# %%
metadata = MetaData()

dic_table = {}
for table_name in engine.table_names():
    dic_table[table_name] = Table(table_name, metadata , autoload=True, autoload_with=engine)
	
print(repr(dic_table['hashtag']))
#%%
from sqlalchemy import select

mapper_stmt = select([dic_table['twitt']])
print('Mapper select: ')
print(mapper_stmt)

session_stmt = session.query(Hashtag)
print('\nSession select: ')
print(session_stmt)
# %%
mapper_results = engine.execute(mapper_stmt).fetchall()
print(mapper_results)
# %% filter by TVN24 bis
mapper_stmt = select([dic_table['twitt'].columns.twitt_real_id.label('id'),dic_table['author'].columns.author_name.label('author_name')]).where(dic_table['author'].columns.author_name == 'TVN24 BiS')
mapper_results = engine.execute(mapper_stmt).fetchall()
print(mapper_results)

# %% Filter by hashtag
mapper_stmt = select([dic_table['twitt'].columns.twitt_real_id.label('id'),dic_table['hashtag'].columns.hashtag_name.label('hashtag_name')]).where(dic_table['hashtag'].columns.hashtag_name == 'włączprawdę')
mapper_results = engine.execute(mapper_stmt).fetchall()
print(mapper_results)
# %%
result = session.query(Hashtag).join(Twitt).all()
print(result)
# %%
