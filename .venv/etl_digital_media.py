#-----------------------------------------DATA EXTRACT-------------------------------------------------
#impoort neccesary libraries
import pandas as pd
import numpy as np
import psycopg2 as ps
from sqlalchemy import create_engine
from sqlalchemy import Text

#extract raw data from datasource
csv_data = pd.read_csv(r"C:\Users\andrz\Desktop\Python\Digital_media.csv")
df_raw = pd.DataFrame(data = csv_data)
df=df_raw


#-----------------------------------------DATA TRANSFORM-------------------------------------------------
df.info()
df.head(10)
replace_string = {'sc-domain:headandshoulders.com.tr':'google.com','sc-domain:headandshoulders.de':'microsoft.com', 'sc-domain:headandshoulders.co.uk':'facebook.com','sc-domain:headandshoulders.pl':'twitter.com','sc-domain:headandshoulders.fr':'amazon.com', 'sc-domain:headandshoulders.it':'allegro.pl'}
df['site_url'] = df['site_url'].str.replace('sc-domain:headandshoulders.com.tr','google.com')
df['site_url'] = df['site_url'].str.replace('sc-domain:headandshoulders.de','microsoft.com')
df['site_url'] = df['site_url'].str.replace('sc-domain:headandshoulders.co.uk','facebook.com')
df['site_url'] = df['site_url'].str.replace('sc-domain:headandshoulders.pl','twitter.com')
df['site_url'] = df['site_url'].str.replace('sc-domain:headandshoulders.fr','amazon.com')
df['site_url'] = df['site_url'].str.replace('sc-domain:headandshoulders.it','allegro.pl')

df['date_new'] = pd.to_datetime(df['date_new']).dt.date
df_final = df
df.info()

#-----------------------------------------DATA LOADING-------------------------------------------------
#establish connection with a database to load data
connection = ps.connect(user = 'postgres', password = '12345', host = '127.0.0.1', port = '5432', database = 'Digital media data')
cur = connection.cursor()
engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Digital media data')
conn = engine.connect()

#load raw data into database
df_raw.to_sql('raw_data', con = conn)
df_final.to_sql('gsc_data', con = conn)

