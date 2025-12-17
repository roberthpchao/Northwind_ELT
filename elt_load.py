import pandas as pd
from sqlalchemy import create_engine, text

# Connection details
SERVER = 'localhost\\SQLEXPRESS01'
DATABASE = 'NORTHWND'
DRIVER = 'ODBC Driver 17 for SQL Server' # Ensure this driver is installed
CONN_STR = f'mssql+pyodbc://@{SERVER}/{DATABASE}?trusted_connection=yes&driver={DRIVER}'

engine = create_engine(CONN_STR)

def load_staging():
    print("Starting Extraction...")
    # Extract Raw Data
    df_orders = pd.read_sql("SELECT * FROM dbo.Orders", engine)
    df_details = pd.read_sql("SELECT * FROM dbo.[Order Details]", engine)
    
    print("Loading to Staging...")
    # Load into Staging Schema (The 'L' in ELT)
    # We replace the table every time for a fresh 'Land'
    df_orders.to_sql('stg_Orders', engine, schema='Staging', if_exists='replace', index=False)
    df_details.to_sql('stg_OrderDetails', engine, schema='Staging', if_exists='replace', index=False)
    
    print("Load Complete.")

if __name__ == "__main__":
    load_staging()