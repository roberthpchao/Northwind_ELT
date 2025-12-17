In data engineering, **Data Quality (DQ) Checks** act as the "security guards" for your data warehouse. You want to catch errors in the **Staging** area *before* they are merged into your final **DWH** tables.

We will implement two types of checks:

1. **Row Count Check:** Did we actually load any data?
2. **Domain Integrity Check:** Are the totals positive? Are the dates valid?

---

###Step 1: Create a Data Quality Log TableRun this in **SSMS**. We need a place to record when a quality check fails.

```sql
USE NORTHWND;
GO

CREATE TABLE Audit.DataQualityLogs (
    QualityID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT,
    TableName NVARCHAR(50),
    CheckName NVARCHAR(100),
    CheckStatus NVARCHAR(10), -- 'PASS' or 'FAIL'
    CheckMessage NVARCHAR(MAX),
    CheckDate DATETIME DEFAULT GETDATE()
);

```

---

###Step 2: Create the DQ Stored ProcedureThis procedure will run inside SQL Server and inspect the `Staging` tables.

```sql
USE NORTHWND;
GO

CREATE OR ALTER PROCEDURE Audit.RunDataQualityChecks
    @BatchID INT
AS
BEGIN
    DECLARE @FailCount INT = 0;

    -- Check 1: Ensure Staging is not empty
    IF (SELECT COUNT(*) FROM Staging.stg_Orders) = 0
    BEGIN
        INSERT INTO Audit.DataQualityLogs (BatchID, TableName, CheckName, CheckStatus, CheckMessage)
        VALUES (@BatchID, 'stg_Orders', 'Empty Table Check', 'FAIL', 'No rows found in Staging.');
        SET @FailCount = @FailCount + 1;
    END

    -- Check 2: Ensure no negative prices
    IF EXISTS (SELECT 1 FROM Staging.stg_OrderDetails WHERE UnitPrice < 0)
    BEGIN
        INSERT INTO Audit.DataQualityLogs (BatchID, TableName, CheckName, CheckStatus, CheckMessage)
        VALUES (@BatchID, 'stg_OrderDetails', 'Negative Price Check', 'FAIL', 'Found rows with UnitPrice below 0.');
        SET @FailCount = @FailCount + 1;
    END

    -- If any checks failed, return an error to Python
    IF @FailCount > 0
    BEGIN
        RAISERROR('Data Quality Checks Failed. See Audit.DataQualityLogs for details.', 16, 1);
    END
    ELSE
    BEGIN
        PRINT 'All Data Quality Checks Passed.';
    END
END;

```

---

###Step 3: Update the Master Script (`elt_audit.py`)Now, we update our main orchestration script to run the DQ checks **after** the Load, but **before** the Transform. If the DQ checks fail, we stop the pipeline.

```python
# ... inside run_pipeline() in elt_audit.py ...

        # 2. Load Staging
        elt_load.load_staging(engine)

        # --- NEW PHASE: DATA QUALITY ---
        print(f"--- Phase: Data Quality (Batch ID: {log_id}) ---")
        with engine.connect() as conn:
            try:
                conn.execute(text("EXEC Audit.RunDataQualityChecks @BatchID = :bid"), {"bid": log_id})
                conn.commit()
            except Exception as dq_err:
                print(f"DQ FAILED: {dq_err}")
                # We raise the error to stop the pipeline before Transformation starts
                raise Exception("Stopping pipeline due to Data Quality failure.")

        # 3. Run Transformation (Only runs if DQ passes!)
        print("--- Phase: Transform ---")
        # ... rest of your code ...

```

---

###Why this is a "Game Changer"In your previous ETL, if you had a bug in your source data, it would flow directly into your analytics. With this ELT Quality Gate:

* **Protection:** The `DWH.Fact_Sales` table remains "clean" because the pipeline crashes before the bad data is moved.
* **Alerting:** You have a specific table (`Audit.DataQualityLogs`) that tells you exactly why the load failed.
* **Trust:** Your end-users (the people looking at reports) can trust that the data is always valid.

###Try it out:1. Run the script. It should pass.
2. Now, manually break the data in your `NORTHWND` source to test it:
```sql
-- Simulate a bad entry
UPDATE [Order Details] SET UnitPrice = -10 WHERE OrderID = 10248;

```


3. Run `python elt_audit.py`.
4. Notice how the script stops! Check your `Audit.DataQualityLogs` to see the error record.

**Would you like me to show you how to "fix" the data automatically during the Transform step if a check fails?** (e.g., *If Price < 0, then set it to 0*)