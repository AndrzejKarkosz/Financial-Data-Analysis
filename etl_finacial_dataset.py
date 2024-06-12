
#-----------------------------------------DATA EXTRACT-------------------------------------------------
#impoort neccesary libraries
import pandas as pd
import numpy as np
import psycopg2 as ps
from sqlalchemy import create_engine
from sqlalchemy import Text

#extract raw data from datasource
csv_data = pd.read_csv(r"C:\Users\andrz\Desktop\Python\Financials.csv")
df_raw = pd.DataFrame(data = csv_data)
df=df_raw


#-----------------------------------------DATA TRANSFORM-------------------------------------------------
df.info()
df.rename(columns = {'index':'id',' Product ':'Product','Country': 'Country',' Discount Band ': 'Discount Band',' Manufacturing Price ': 'Manufacturing Price',' Sale Price ': 'Sale Price',' Gross Sales ': 'Gross Sales',' Discounts ': 'Discounts','  Sales ': 'Sales', ' COGS ': 'COGS',' Profit ': 'Profit', ' Month Name ':'Month Name', ' Units Sold ':'Units Sold'}, inplace=True)

#changing data types
df = df.astype({'Segment':'string','Country':'string','Product': 'string','Discount Band': 'string','Manufacturing Price': 'string','Sale Price': 'string','Gross Sales': 'string','Discounts': 'string','Sales': 'string','COGS': 'string','Profit': 'string'})

#replacing characters and changing dtype = float
df['COGS'] = df['COGS'].str.replace('$',"").str.replace(' ','').str.replace(',','').astype(float)
df['Units Sold'] = df['Units Sold'].str.replace('$',"").str.replace(' ','').str.replace(',','').astype(float)
df['Discounts'] = df['Discounts'].str.replace('$',"").str.replace(' ','').str.replace(',','').str.replace('-',"0").astype(float)
df['Manufacturing Price'] = df['Manufacturing Price'].str.replace('$',"").str.replace(' ','').str.replace(',','').astype(float)
df['Sale Price'] = df['Sale Price'].str.replace('$',"").str.replace(' ','').str.replace(',','').astype(float)
df['Gross Sales'] = df['Gross Sales'].str.replace('$',"").str.replace(' ','').str.replace(',','').astype(float)
df['Discounts'] = df['Discounts'].str.replace('$',"").str.replace(' ','').str.replace(',','').astype(float)
df['Sales'] = df['Sales'].str.replace('$',"").str.replace(' ','').str.replace(',','').astype(float)
df['Profit'] = df['Profit'].str.replace('$',"").str.replace(' ','').str.replace(',','')
df['Discounts'].head(10)
df['Date'] = pd.to_datetime(df['Date']).dt.date

#droping columns that needs to be recalculated
df = df.drop('Profit',axis =1)
df = df.drop('Gross Sales',axis =1)
df = df.drop('Sales',axis =1)

#recalculating columns
df['Gross Sales'] = df['Units Sold']*df['Sale Price']
df['Sales'] = df['Gross Sales']-df['Discounts']
df['Profit'] = df['Sales']-df['COGS']

df.index = df.index +1
df.info()

#creating tables
df_sales = df[['Segment', 'Country', 'Product', 'Units Sold', 'Gross Sales', 'Sale Price', 'Sales','Date']]
df_cost = df[['Segment', 'Country', 'Product', 'Units Sold', 'Manufacturing Price', 'COGS','Date']]
df_master = df


#-----------------------------------------DATA LOADING-------------------------------------------------
#establish connection with a database to load data
connection = ps.connect(user = 'postgres', password = 'XXXX', host = 'XXX.X.X.X', port = '5432', database = 'Financial data')
cur = connection.cursor()
engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Financial data')
conn = engine.connect()

#load raw data into database
df_raw.to_sql('raw_data', con = conn, if_exists='replace')
df_sales.to_sql('sales_data',con = conn, if_exists='replace')
df_cost.to_sql('cost_data',con = conn, if_exists='replace')
df_master.to_sql('master_data',con= conn, if_exists='replace')

#close connection
conn.close()




