#https://www.youtube.com/watch?v=Y1OFbez9qK0
import pyodbc as podbc
import pandas as pd
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
import socket

DRIVER_NAME = 'SQL Server'
SERVER_NAME = 'BUJJIS'
DATABASE_NAME = 'MYDBS'

def sql_connection(mssql_driver,mssql_server,mssql_db):
    try:
        sql_conn_str = "DRIVER={};SERVER={};DATABASE={};TRUSTED_CONNECTION=Yes".format(mssql_driver,mssql_server,mssql_db)
        conn = podbc.connect(sql_conn_str)
        
        return (True, conn)
    except:
        err_cd = "IF-PP-0006"
        return ("error",err_cd)
		
		
value = sql_connection(DRIVER_NAME,SERVER_NAME,DATABASE_NAME)
conn = value[1]
cursor = conn.cursor()

sql_query = pd.read_sql('''select * from UserInfo''',conn)
#sql_query = pd.read_sql_query('''select * from Raw_Data_GDP''',conn)
sql_query.head()


#Insert values into table

sql_query = "Insert Into UserInfo values (?,?,?,?,?,?)"
values = ("1103","Thari","Petrons","821540","1987-09-12","2009-12-15")

cursor.execute(sql_query, (values))
cursor.commit()

#Reading from CSV and Bulk Insert into SQL Server table - Raw_Data_GDP
csvPath = "D:\Study\Python\SQLServer_Python\Raw_Data_GDP.csv"
df = pd.read_csv(csvPath)
#df.head()

#connBulk = sqlalchemy.create_engine(f'mssql+pyodbc://{socket.gethostname()}/MYDBS?trusted_connection=yes&driver=SQL Server')
connBulk = sqlalchemy.create_engine(f'mssql+pyodbc://'+SERVER_NAME+'/'+DATABASE_NAME+'?driver='+DRIVER_NAME+'&trusted_connection=yes')
df.to_sql("Raw_Data_GDP", con = connBulk,if_exists="append", index=False )		