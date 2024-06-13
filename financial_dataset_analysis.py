

#Import neccesary libraries
import pandas as pd
import numpy as np
import psycopg2 as ps
from sqlalchemy import create_engine
from sqlalchemy import text
import matplotlib.pyplot as plt
#____________________________________________________________________________________________________________________________________________________________________
                                                                        #Chart classes
#Plot over time
class Over_time_plot:
    def __init__(self, x_axis, y_axis, x_label, y_label,title):
        self.x_axis = x_axis
        self.y_axis = y_axis
        self.x_label = x_label
        self.y_label = y_label
        self.title = title

        plt.plot(x_axis, y_axis)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.show()

#Bar Chart
class Bar_plot:
    def __init__(self, x_axis, y_axis, x_label, y_label,title):
        self.x_axis = x_axis
        self.y_axis = y_axis
        self.x_label = x_label
        self.y_label = y_label
        self.title = title

        plt.bar(x_axis, y_axis)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.show()

#____________________________________________________________________________________________________________________________________________________________________
#Establish connection with database
connection = ps.connect(user = 'postgres', password = 'xxxx', host = 'xxxxxxx', port = '5432', database = 'Financial data')
con_engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Financial data')
con = con_engine.raw_connection()
cursor = con.cursor()
cursor_2 = connection.cursor()

#Load data from database:

sales_df = pd.read_sql_query('SELECT * FROM sales_data',con_engine)
cost_df = pd.read_sql_query('SELECT * FROM cost_data',con_engine)
master_df = pd.read_sql_query('SELECT * FROM master_data',con_engine)

#--------------------------------------------------SALES DATA BASIC ANALYSIS------------------------------------------------

# Question: Which segment has the biggest sales?
segment_sales = sales_df[['Segment','Sales']].groupby(['Segment']).sum('Sales').sort_values(by = 'Sales', ascending= False).reset_index()
segment_sales_anwser = segment_sales['Segment'][0]
# Anwser:
anwser_sales_q1 = print('The biggest segment in terms of sales is ' + segment_sales_anwser)
anwser_sales_q1

# Question: Which country has the biggest sales?
country_sales = sales_df[['Country','Sales']].groupby(['Country']).sum('Sales').sort_values(by = 'Sales', ascending= False).reset_index()
country_sales_anwser = country_sales['Country'][0]
# Answser: 
anwser_sales_q2 = print('The biggest country in terms of sales is ' + country_sales_anwser)
anwser_sales_q2

# Question: What is the best selling product in terms of quantity? 
product_total_sales = sales_df[['Product','Units Sold']].groupby(['Product']).sum('Units Sold').sort_values(by = 'Units Sold', ascending= False).reset_index()
product_total_sales_anwser = product_total_sales['Product'][0]
# Anwser: 
anwser_sales_q3 = print('The best selling product ' + product_total_sales_anwser)
anwser_sales_q3

# Question: What is the most expensive product? 
product_sale_price = sales_df[['Product','Sale Price']].groupby(['Product']).max('Sale Price').sort_values(by = 'Sale Price', ascending= False).reset_index()
product_sale_price_anwser = product_sale_price['Product'][0]
# Anwser: 
anwser_sales_q4 = print('The best selling product ' + product_total_sales_anwser)
anwser_sales_q4

# Question: Is there any seasonality in sales? 
group_sales_df = sales_df[['Date','Sales']].groupby('Date')['Sales'].sum().reset_index()
group_sales_df = pd.DataFrame(group_sales_df)
        
# Anwser: 
Over_time_plot(group_sales_df['Date'],group_sales_df['Sales'], 'Date','Total Sales', 'Seasonality of sales')



#--------------------------------------------------COST DATA BASIC ANALYSIS------------------------------------------------
# Question: Which segment generates the most costs? 
segment_cost = cost_df[['Segment','COGS']].groupby('Segment').sum('COGS').sort_values(by = 'COGS', ascending=False).reset_index()
segment_cost_anwser = segment_cost['Segment'][0]
# Anwser: 
anwser_cost_q1 = print('Segment which generates the most costs is ' + segment_cost_anwser)
anwser_cost_q1

# Question: Which country generates the most costs? 
country_cost = cost_df[['Country','COGS']].groupby('Country').sum('COGS').sort_values(by = 'COGS', ascending=False).reset_index()
country_cost_anwser = country_cost['Country'][0]
# Anwser: 
anwser_cost_q2 = print('Country which generates the most costs is ' + country_cost_anwser)
anwser_cost_q2


#--------------------------------------------------PROFIT BASIC ANALYSIS----------------------------------------------------
# Question: Which country is most profitable? 
profit_country_df = master_df[['Country', 'Sales','COGS']].groupby('Country').sum(['Sales','COGS']).reset_index()
profit_country_df['Profit'] = profit_country_df['Sales'] - profit_country_df['COGS']
profit_country_df.sort_values(by = 'Profit', ascending=False)
profit_top1_anwser = profit_country_df['Country'][0]
profit_bottom1 = profit_country_df.sort_values(by = 'Profit', ascending=True).reset_index()
profit_bottom1_anwser = profit_bottom1['Country'][0]
anweser_top1_profit = print(f'{profit_top1_anwser} is the most profitable country')
anweser_bottom1_profit = print(f'{profit_bottom1_anwser} is the least profitable country')

#Anwser 
Bar_plot(profit_country_df['Country'], profit_country_df['Profit'],'Countries','Profit','Profit by countries')

# Question: Which segment is the most profitable? 
profit_segment_df = master_df[['Segment', 'Sales','COGS']].groupby('Segment').sum(['Sales','COGS']).reset_index()
profit_segment_df['Profit'] = profit_segment_df['Sales'] - profit_segment_df['COGS']
profit_segment_df.sort_values(by = 'Profit', ascending=False)
profit_top1_segment_anwser = profit_segment_df['Segment'][0]
profit_bottom1_segment = profit_segment_df.sort_values(by = 'Profit', ascending=True).reset_index()
profit_bottom1_segment_anwser = profit_bottom1_segment ['Segment'][0]
anweser_top1_profit = print(f'{profit_top1_segment_anwser} is the most profitable segment')
anweser_bottom1_profit = print(f'{profit_bottom1_segment_anwser } is the least profitable segment')

#Anwser 
Bar_plot(profit_segment_df['Segment'], profit_country_df['Profit'],'Segments','Profit','Profit by segments')

# Question: Which products have the best margain?
profit_product_df = master_df[['Product', 'Sales','COGS','Units Sold']].groupby('Product').sum(['Sales','COGS','Units Sold']).reset_index()
profit_product_df['Profit'] = profit_product_df['Sales'] - profit_product_df['COGS']
profit_product_df.sort_values(by = 'Profit', ascending=False)
profit_product_df['Margin'] = profit_product_df['Profit'] / profit_product_df['Sales'] 
profit_product_df.sort_values(by = 'Margin', ascending=False)
profit_top1_product_anwser = profit_product_df['Product'][0]
profit_bottom1_product = profit_product_df.sort_values(by = 'Margin', ascending=True).reset_index()
profit_bottom1_product_anwser = profit_bottom1_product['Product'][0]
anweser_top1_margin = print(f'{profit_top1_product_anwser} has the best margin in products')
anweser_bottom1_margin = print(f'{profit_bottom1_product_anwser } has the worst margin in products')

# Anwser: We can see that the profit margin is the lowest for the Velo product, which means that we need to examine whether the price of the product needs to be raised
Bar_plot(profit_product_df['Product'], profit_product_df['Margin'],'Segments','Margin','Margin by segments')


# Are product selling prices correct in each segment?
units_sold_product_margin_df = master_df[['Product', 'Segment','Units Sold']].groupby(['Product', 'Segment']).sum(['Units Sold']).reset_index()
man_prices_product_margin_df = master_df[['Product', 'Segment','Manufacturing Price']].groupby(['Product', 'Segment']).max(['Manufacturing Price']).reset_index()
sale_prices_product_margin_df = master_df[['Product', 'Segment','Sale Price']].groupby(['Product', 'Segment']).max(['Sale Price']).reset_index()
unit_margin_df = units_sold_product_margin_df.merge(sale_prices_product_margin_df, how = 'left', on = ['Product','Segment']).merge(man_prices_product_margin_df, how = 'left', on = ['Product','Segment'])
unit_margin_df['Price_diff'] = unit_margin_df['Sale Price'] - unit_margin_df['Manufacturing Price']

# Anwser to pricing problem
for index, row in unit_margin_df.iterrows():
    Product = row['Product']
    Segment = row['Segment']
    Price_diff = row['Price_diff']
   
    if Price_diff < 0:
        print(f'The price for {Product} product, in {Segment} segment, should be increased by {Price_diff*-1}')
    else: 
        print(f'The price for {Product} product, in {Segment} segment is suitable')


