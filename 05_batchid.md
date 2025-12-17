Adding a **Batch ID** is the final piece of the puzzle for a professional ELT system. It allows you to link every single row in your data warehouse back to the specific execution in the `Audit.PipelineLog` table.

If you find a data error tomorrow, you can say, *"This error only exists in Batch #10,"* and you'll know exactly when it happened.

###Step 1: Update your Tables in SSMSWe need to add a `BatchID` column to our target table so it can store the reference.

```sql
USE NORTHWND;
GO

-- Add BatchID column to the Fact table
ALTER TABLE DWH.Fact_Sales ADD BatchID INT;

-- Update the Stored Procedure to accept the BatchID as a parameter
CREATE OR ALTER PROCEDURE DWH.TransformSales 
    @CurrentBatchID INT
AS
BEGIN
    TRUNCATE TABLE DWH.Fact_Sales;

    INSERT INTO DWH.Fact_Sales (OrderID, CustomerName, TotalAmount, OrderDate, BatchID)
    SELECT 
        o.OrderID,
        UPPER(o.CustomerID),
        SUM(d.UnitPrice * d.Quantity),
        o.OrderDate,
        @CurrentBatchID  -- We pass the ID here
    FROM Staging.stg_Orders o
    JOIN Staging.stg_OrderDetails d ON o.OrderID = d.OrderID
    GROUP BY o.OrderID, o.CustomerID, o.OrderDate;
END;

```

---

###Step 2: Update the Main Script (`elt_audit.py`)Now, we capture the `log_id` (which is our Batch ID) and pass it into the stored procedure.

```python
# ... (inside your run_pipeline function in elt_audit.py) ...

        # 3. Run Transformation with Batch ID
        print(f"--- Phase: Transform (Batch ID: {log_id}) ---")
        with engine.connect() as conn:
            # Passing the log_id as the @CurrentBatchID parameter
            sql_cmd = text("EXEC DWH.TransformSales @CurrentBatchID = :bid")
            conn.execute(sql_cmd, {"bid": log_id})
            
            count_res = conn.execute(text("SELECT COUNT(*) FROM DWH.Fact_Sales"))
            row_count = count_res.fetchone()[0]
            conn.commit()

# ... (rest of the script) ...

```

---

###Step 3: See the ResultAfter running `python elt_audit.py`, you can now perform a "Data Lineage" query in SSMS:

```sql
-- Join the data to the audit log to see when these specific rows were loaded
SELECT 
    F.OrderID, 
    F.TotalAmount, 
    A.StartTime as LoadedAt, 
    A.Status
FROM DWH.Fact_Sales F
JOIN Audit.PipelineLog A ON F.BatchID = A.LogID;

```

###Summary of what you've built:1. **Staging Area:** A place for raw data.
2. **Modular Python:** Clean, organized code.
3. **Stored Procedures:** Fast SQL-based transformations.
4. **Audit Trail:** A history of every run.
5. **Data Lineage:** Every row of data knows who its "parent" run was.

You have now moved from **Basic ETL** to a **Professional ELT Architecture**.

**Would you like me to help you create a "Cleaning" script that automatically deletes old Audit logs or Staging data older than 30 days?**S