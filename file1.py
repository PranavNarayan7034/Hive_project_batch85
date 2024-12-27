from pyhive import hive
# Connection to hive
db = 'batch85'
crs = hive.connect(host='localhost',database=db).cursor()
# column info
columninfo = "InvoiceNo int,StockCode string,Description string,Quantity int,InvoiceDate string,UnitPrice float,CustomerID float,Country string,Discount float,PaymentMethod string,ShippingCost float,\
Category string,SalesChannel string,ReturnStatus string,ShipmentProvider string,WarehouseLocation string,OrderPriority string"
#table creation
crs.execute(f'Create table if not exists Online_sales({columninfo})row format \
delimited fields terminated by "," tblproperties("skip.header.line.count"="1")')
crs.execute('show tables')
tables = crs.fetchall()
print(f'Your tables in database:{db}')
for i in tables:
    print(i)      #tables present inside database;

# data loading and crosscheck
try:
    crs.execute('select * from Online_sales limit 5')
    data = crs.fetchall()
    if len(data)==0:
        filepath = 'hdfs://localhost:9000/hive_project/online_sales_dataset.csv'
        crs.execute(f"load data inpath '{filepath}' overwrite into table Online_sales")
        print('Data loaded Successfully')
    else:
        # filepath  = 'hdfs://localhost:9000/user/hive/warehouse/batch85.db/online_sales/online_sales_dataset.csv'
        # crs.execute(f"load data inpath '{filepath}' overwrite into table Online_sales")
        print('Data is already Available in table')

except Exception:
    print('Error while loading data into table...!')

# Analytical problems
# 1) No.of payment methods in each country :

crs.execute('select country,paymentmethod,count(*)as no_of_transactions from Online_sales\
 group by Country,paymentmethod')
out = crs.fetchall()
# print(out)

x_column = 'country'
crs.execute(f'select distinct {x_column} from Online_sales')
x = crs.fetchall()

import numpy as np
x_values = np.arange(1,len(x)+1)
# print(x_values)

BT = []
CC = []
PP = []
for i in out:
    print(i)
    if i[1] == 'Bank Transfer':
        BT.append(i[2])
    elif i[1] == 'Credit Card':
        CC.append(i[2])
    elif i[1] == 'paypall':
        PP.append(i[2])

print('****************')
print(x)
print(BT)
print(CC)
print(PP)
