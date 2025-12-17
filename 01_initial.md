###The ELT Workflow1. **Extract:** Pull raw data from `NORTHWND` tables using Python.
2. **Load:** Push that data into a "Landing" or "Staging" schema in SQL Server.
3. **Transform:** Run SQL stored procedures to clean and model the data into a Final/Analytics schema.

---

##Step 1: Project SetupOpen your terminal or VS Code and set up your environment:

1. Create the folder: `mkdir Northwind_ELT`
2. Navigate into it: `cd Northwind_ELT`
3. Install the necessary library:
```bash
pip install pandas sqlalchemy pyodbc

```



---

##Step 2: Prepare the Database (Staging vs. Production)Open **SSMS** and run this script. We are creating a **Staging** schema to hold the "messy" raw data and a **DWH** (Data Warehouse) schema for the "clean" data.

```sql
USE NORTHWND;
GO

-- Create separate schemas for ELT stages
CREATE SCHEMA Staging;
GO
CREATE SCHEMA DWH;
GO

-- A simple target table for our transformation example
CREATE TABLE DWH.Fact_Sales (
    OrderID INT,
    CustomerName NVARCHAR(50),
    TotalAmount DECIMAL(18,2),
    OrderDate DATETIME
);

```

---

##Step 3: The Python "Load" ScriptThis script performs the **E** and **L**. It takes raw data from the original tables and dumps them exactly as they are into the `Staging` schema.

Create a file named `elt_load.py`:

```python
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

```

---

##Step 4: The SQL "Transform" ScriptNow that the data is inside the database, we use SQL to transform it. This is the **T** in ELT.

Open **SSMS** and create this Stored Procedure:

```sql
USE NORTHWND;
GO

CREATE OR ALTER PROCEDURE DWH.TransformSales
AS
BEGIN
    -- Clear the target table
    TRUNCATE TABLE DWH.Fact_Sales;

    -- Transform and Move data from Staging to DWH
    INSERT INTO DWH.Fact_Sales (OrderID, CustomerName, TotalAmount, OrderDate)
    SELECT 
        o.OrderID,
        UPPER(o.CustomerID) as CustomerName, -- Simple transformation
        SUM(d.UnitPrice * d.Quantity) as TotalAmount,
        o.OrderDate
    FROM Staging.stg_Orders o
    JOIN Staging.stg_OrderDetails d ON o.OrderID = d.OrderID
    GROUP BY o.OrderID, o.CustomerID, o.OrderDate;
    
    PRINT 'Transformation Complete.';
END;

```

---

##Step 5: Execute the PipelineTo run your full ELT process:

1. **Run Python:** `python elt_load.py` (This moves data to Staging).
2. **Run SQL:** In SSMS, execute: `EXEC DWH.TransformSales;`
3. **Verify:** `SELECT * FROM DWH.Fact_Sales;`

---

###Why is this better than ETL?In your previous ETL practice, Python likely did the `SUM()` and the `JOIN`. In **ELT**:

* **Python** acts as the "transport truck" (moving data).
* **SQL Server** acts as the "factory" (transforming data).
* This is much faster when dealing with millions of rows because SQL Server is optimized for joining large datasets.