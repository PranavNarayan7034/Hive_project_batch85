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
        print('Data is already Available in table')

except Exception:
    print('Error while loading data into table...!')


# Analytical problems









