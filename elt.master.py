import pandas as pd
from sqlalchemy import create_engine, text

# Connection details
SERVER = 'localhost\\SQLEXPRESS01'
DATABASE = 'NORTHWND'
DRIVER = 'ODBC Driver 17 for SQL Server' 
CONN_STR = f'mssql+pyodbc://@{SERVER}/{DATABASE}?trusted_connection=yes&driver={DRIVER}'

engine = create_engine(CONN_STR)

def run_elt_pipeline():
    try:
        # --- PHASE 1: EXTRACT ---
        print("1. Extracting raw data from dbo...")
        df_orders = pd.read_sql("SELECT * FROM dbo.Orders", engine)
        df_details = pd.read_sql("SELECT * FROM dbo.[Order Details]", engine)

        # --- PHASE 2: LOAD ---
        print("2. Loading raw data into Staging schema...")
        df_orders.to_sql('stg_Orders', engine, schema='Staging', if_exists='replace', index=False)
        df_details.to_sql('stg_OrderDetails', engine, schema='Staging', if_exists='replace', index=False)

        # --- PHASE 3: TRANSFORM ---
        print("3. Triggering SQL Transformation (Stored Procedure)...")
        with engine.connect() as connection:
            # We use .execution_options(autocommit=True) to ensure the procedure runs
            connection.execute(text("EXEC DWH.TransformSales"))
            connection.commit()
            
        print("SUCCESS: ELT Pipeline completed.")

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    run_elt_pipeline()