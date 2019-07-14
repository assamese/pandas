#!/usr/bin/env python
# coding: utf-8

# In[14]:


# Read reference-data (sourced from Vendor and available in ) S3 and dump into a Postgresql table
import pandas as pd
from smart_open import smart_open
import os

aws_key = 'xxx'
aws_secret = 'xxx'

bucket_name = 'marketdata-sanjay'
object_key = 'market-reference-data.csv'

################### sample CSV data ##################################
# Name,Sector,Revenues,EBITDA,Net_Margin,MCAP,Ticker
# 1 800 FLOWERS COM INC,Retail - Apparel & Specialty,1164,70,3.2%,1244,FLWS
# 1ST SOURCE CORP,Banks,316,116,27.1%,1211,SRCE
# 21st Century Fox,Entertainment,30575,7053,17.0%,67730,FOX
##############################################################################

path = 's3://{}:{}@{}/{}'.format(aws_key, aws_secret, bucket_name, object_key)

df = pd.read_csv(smart_open(path))


# In[15]:


# strip %sign
df["Net_Margin"] = df["Net_Margin"].str.replace('%', '')
# convert type from String to Numeric
df["Net_Margin"] = pd.to_numeric(df["Net_Margin"])


# In[17]:


df.head()


# In[18]:


# dump tramnformed/cleaned ref data into Postgresql
import sqlalchemy
from sqlalchemy import create_engine 
# Postgres username, password, and database name 
POSTGRES_ADDRESS = 'raja.db.elephantsql.com' ## INSERT YOUR DB ADDRESS 
POSTGRES_PORT = '5432' 
POSTGRES_USERNAME = 'ljalsmbf' 
POSTGRES_PASSWORD = 'xxx-xxx' ## CHANGE THIS TO YOUR POSTGRES PASSWORD 
POSTGRES_DBNAME = 'ljalsmbf' ## CHANGE THIS TO YOUR DATABASE NAME 
# A long string that contains the necessary Postgres login information 
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME, password=POSTGRES_PASSWORD, ipaddress=POSTGRES_ADDRESS, port=POSTGRES_PORT, dbname=POSTGRES_DBNAME)) 
# Create the connection 
cnx = create_engine(postgres_str)


# In[19]:


df.to_sql('market_reference_data_test', cnx)


# In[ ]:




