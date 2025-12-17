import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Connection details
SERVER = 'localhost\\SQLEXPRESS01'
DATABASE = 'NORTHWND'
DRIVER = 'ODBC Driver 17 for SQL Server' 
CONN_STR = f'mssql+pyodbc://@{SERVER}/{DATABASE}?trusted_connection=yes&driver={DRIVER}'
engine = create_engine(CONN_STR)

def run_elt_pipeline():
    start_time = datetime.now()
    pipeline_name = "Northwind_Sales_ELT"
    log_id = None
    
    try:
        # --- LOG START ---
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                INSERT INTO Audit.PipelineLog (PipelineName, StartTime, Status) 
                VALUES ('{pipeline_name}', '{start_time}', 'Running');
                SELECT SCOPE_IDENTITY();
            """))
            log_id = result.fetchone()[0]
            conn.commit()

        # --- PHASE 1 & 2: EXTRACT & LOAD ---
        print("Extracting and Loading...")
        df_orders = pd.read_sql("SELECT * FROM dbo.Orders", engine)
        df_details = pd.read_sql("SELECT * FROM dbo.[Order Details]", engine)
        
        df_orders.to_sql('stg_Orders', engine, schema='Staging', if_exists='replace', index=False)
        df_details.to_sql('stg_OrderDetails', engine, schema='Staging', if_exists='replace', index=False)

        # --- PHASE 3: TRANSFORM ---
        print("Transforming...")
        with engine.connect() as conn:
            conn.execute(text("EXEC DWH.TransformSales"))
            # Get row count for the log
            count_res = conn.execute(text("SELECT COUNT(*) FROM DWH.Fact_Sales"))
            row_count = count_res.fetchone()[0]
            conn.commit()

        # --- LOG SUCCESS ---
        end_time = datetime.now()
        with engine.connect() as conn:
            conn.execute(text(f"""
                UPDATE Audit.PipelineLog 
                SET EndTime = '{end_time}', Status = 'Success', RowsProcessed = {row_count}
                WHERE LogID = {log_id}
            """))
            conn.commit()
        print(f"SUCCESS: {row_count} rows processed.")

    except Exception as e:
        # --- LOG FAILURE ---
        print(f"ERROR: {e}")
        with engine.connect() as conn:
            if log_id:
                conn.execute(text(f"""
                    UPDATE Audit.PipelineLog 
                    SET EndTime = '{datetime.now()}', Status = 'Error', ErrorMessage = '{str(e).replace("'", "''")}'
                    WHERE LogID = {log_id}
                """))
                conn.commit()

if __name__ == "__main__":
    run_elt_pipeline()