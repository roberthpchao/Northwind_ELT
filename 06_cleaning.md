Maintaining a clean database is just as important as building the pipeline. Over time, your `Staging` tables and `Audit.PipelineLog` will grow, potentially slowing down your server. We will create a "Maintenance" script to keep things tidy.

###Step 1: The Cleanup Stored ProcedureRun this in **SSMS**. This procedure will delete logs older than 30 days and truncate (empty) the staging tables to save space.

```sql
USE NORTHWND;
GO

CREATE OR ALTER PROCEDURE Audit.CleanUpMetadata
    @DaysToKeep INT = 30
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Remove old logs
    DELETE FROM Audit.PipelineLog 
    WHERE StartTime < DATEADD(day, -@DaysToKeep, GETDATE());

    -- 2. Optional: Completely wipe staging after a successful run
    -- (Only do this if you don't need to debug the raw data later)
    TRUNCATE TABLE Staging.stg_Orders;
    TRUNCATE TABLE Staging.stg_OrderDetails;

    PRINT 'Cleanup complete: Old logs removed and staging cleared.';
END;

```

---

###Step 2: Create the Maintenance Script (`elt_maintenance.py`)This script can be run independently (perhaps once a week) or added to the end of your main pipeline.

```python
from sqlalchemy import create_engine, text

SERVER = 'localhost\\SQLEXPRESS01'
DATABASE = 'NORTHWND'
DRIVER = 'ODBC Driver 17 for SQL Server'
CONN_STR = f'mssql+pyodbc://@{SERVER}/{DATABASE}?trusted_connection=yes&driver={DRIVER}'
engine = create_engine(CONN_STR)

def run_maintenance(days_to_keep=30):
    print(f"--- Phase: Maintenance (Cleaning older than {days_to_keep} days) ---")
    try:
        with engine.connect() as conn:
            # We pass the parameter to the SQL procedure
            conn.execute(text("EXEC Audit.CleanUpMetadata @DaysToKeep = :d"), {"d": days_to_keep})
            conn.commit()
        print("Maintenance Success.")
    except Exception as e:
        print(f"Maintenance Failed: {e}")

if __name__ == "__main__":
    run_maintenance()

```

---

###Your Professional Project FolderYour `Northwind_ELT` folder should now look like a real-world data project:

| File | Purpose |
| --- | --- |
| **`elt_load.py`** | **Extract & Load:** Moves raw data into SQL. |
| **`elt_audit.py`** | **Orchestrator:** The "Main" script that logs and runs the steps. |
| **`elt_maintenance.py`** | **Janitor:** Cleans up old logs and staging data. |

###How to use this for a Daily Job:In a real job, you would use a "Task Scheduler" (Windows) or "Airflow" (Linux/Cloud) to trigger `elt_audit.py` every night at 2:00 AM and `elt_maintenance.py` every Sunday night.

###Final Check1. Run `python elt_audit.py` to ensure data is fresh.
2. Run `python elt_maintenance.py` to see the "Janitor" work.
3. Check **SSMS** `Audit.PipelineLog` to see your history.

**You've built a full-scale ELT framework!** Would you like me to show you how to generate a simple **Markdown Report** from Python that summarizes the last 7 days of pipeline performance (Success vs. Failures)?