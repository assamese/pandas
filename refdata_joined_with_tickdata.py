#!/usr/bin/env python
# coding: utf-8

# In[3]:


#!/usr/bin/env python
# coding: utf-8

# In[29]:


# Read Tick data from 2 separate S3 files and concat to create a single tick-Dataframe
# Read ref-data from Postgresql into ref-Dataframe
# Join tick-Dataframe with ref-Dataframe

import pandas as pd
from smart_open import smart_open
import os

aws_key = 'xxx'
aws_secret = 'xxx'

# Read Tick Data for AAPL
############ Sample CSV Data ##############################################
# Gmt time,Ask,Bid,AskVolume,BidVolume
# 01.07.2019 13:30:03.304,203.14900000000003,203.107,0.0002,0.0001
# 01.07.2019 13:30:03.354,203.21200000000002,203.158,0.0002,0.0001
# 01.07.2019 13:30:03.455,203.14900000000003,203.05700000000002,0.0002,0.01
# 01.07.2019 13:30:03.540,203.148,203.05700000000002,0.0002,0
##########################################################################
bucket_name = 'marketdata-sanjay'
object_key = 'AAPL.USUSD_Ticks_30.06.2019-30.06.2019.csv'

path = 's3://{}:{}@{}/{}'.format(aws_key, aws_secret, bucket_name, object_key)

dfAAPL = pd.read_csv(smart_open(path))


# In[30]:


# Add an additional column to the dataframe
dfAAPL['Ticker'] = 'AAPL'


# In[33]:


dfAAPL.head ()


# In[34]:


# Read Tick Data for IBM
object_key = 'IBM.USUSD_Ticks_30.06.2019-30.06.2019.csv'

path = 's3://{}:{}@{}/{}'.format(aws_key, aws_secret, bucket_name, object_key)

dfIBM = pd.read_csv(smart_open(path))


# In[35]:


dfIBM['Ticker'] = 'IBM'


# In[36]:


dfIBM.count()


# In[22]:


df_aapl_ibm = pd.concat([dfAAPL, dfIBM])


# In[27]:


df_aapl_ibm = df_aapl_ibm.reset_index()


# In[37]:


# Read ref-data from postgresql 
import sqlalchemy

from sqlalchemy import create_engine 
# Postgres username, password, and database name 
POSTGRES_ADDRESS = 'raja.db.elephantsql.com' ## INSERT YOUR DB ADDRESS 
POSTGRES_PORT = '5432' 
POSTGRES_USERNAME = 'ljalsmbf' 
POSTGRES_PASSWORD = 'xxx-xxx' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD 
POSTGRES_DBNAME = 'ljalsmbf' ## CHANGE THIS TO YOUR DATABASE NAME 
# A long string that contains the necessary Postgres login information 
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME, password=POSTGRES_PASSWORD, ipaddress=POSTGRES_ADDRESS, port=POSTGRES_PORT, dbname=POSTGRES_DBNAME)) 
# Create the connection 
cnx = create_engine(postgres_str)

df_ref_data = pd.read_sql_query('''select * from public."market_reference_data_test";''', cnx)

df_ref_data.head()


# In[5]:


df_aapl_ibm.sample(10)


# In[6]:


df_join_inner = pd.merge(df_ref_data, df_aapl_ibm, on='Ticker', how='inner')


# In[8]:


df_join_inner.sample(25)


# In[ ]:




