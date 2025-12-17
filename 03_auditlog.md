An **Audit Log** is a professional standard in data engineering. It helps you answer the question: *"Did the data load correctly last night, and if not, why?"*

We will create a table to track every time the pipeline runs.

---

##Step 1: Create the Audit TableRun this in **SSMS** to create a dedicated schema and table for tracking your jobs.

```sql
USE NORTHWND;
GO

CREATE SCHEMA Audit;
GO

CREATE TABLE Audit.PipelineLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    PipelineName NVARCHAR(100),
    StartTime DATETIME,
    EndTime DATETIME,
    Status NVARCHAR(50), -- 'Success' or 'Error'
    RowsProcessed INT,
    ErrorMessage NVARCHAR(MAX)
);

```

---

##Step 2: Update the Python ScriptWe will now modify the `run_elt_pipeline()` function. It will:

1. **Insert** a "Started" record at the beginning.
2. **Update** that record with "Success" and the row count at the end.
3. **Log the error** if something crashes.

Update your `elt_load.py`:

```python
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

```

---

##Step 3: Verify the AuditRun the script using `python elt_load.py`. Then, check your log table in SSMS:

```sql
SELECT * FROM Audit.PipelineLog ORDER BY StartTime DESC;

```

###Why this is a "Junior to Mid-Level" Skill:1. **Traceability:** You can now see exactly when the data was last updated.
2. **Error Handling:** If the database goes offline or a table name changes, the `ErrorMessage` column will tell you exactly what happened without you having to hunt through terminal logs.
3. **Performance Tracking:** By looking at `StartTime` vs `EndTime`, you can see if your pipeline is getting slower over time as the database grows.

**Would you like me to show you how to create a simple Power BI or Excel connection to visualize these "Fact_Sales" results?**