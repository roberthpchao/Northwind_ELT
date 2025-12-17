Generating a **Markdown Report** is a fantastic way to communicate the health of your data pipeline to stakeholders (or just to keep a record for yourself). We will use Python to query the `Audit` table, calculate some statistics, and save them into a clean `.md` file.

###Step 1: Create the Reporting Script (`elt_report.py`)This script will calculate the success rate and row counts for the last 7 days.

```python
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Connection Setup
SERVER = 'localhost\\SQLEXPRESS01'
DATABASE = 'NORTHWND'
DRIVER = 'ODBC Driver 17 for SQL Server'
CONN_STR = f'mssql+pyodbc://@{SERVER}/{DATABASE}?trusted_connection=yes&driver={DRIVER}'
engine = create_engine(CONN_STR)

def generate_markdown_report():
    print("Generating Pipeline Performance Report...")
    
    # Query the last 7 days of audit data
    query = """
    SELECT 
        CAST(StartTime AS DATE) as RunDate,
        Status,
        COUNT(*) as RunCount,
        SUM(ISNULL(RowsProcessed, 0)) as TotalRows
    FROM Audit.PipelineLog
    WHERE StartTime >= DATEADD(day, -7, GETDATE())
    GROUP BY CAST(StartTime AS DATE), Status
    ORDER BY RunDate DESC
    """
    
    df = pd.read_sql(query, engine)
    
    # Create the Markdown Content
    report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    md_content = f"# Northwind ELT Performance Report\n"
    md_content += f"*Generated on: {report_time}*\n\n"
    
    md_content += "## Summary Table\n"
    # Convert dataframe to markdown table
    md_content += df.to_markdown(index=False)
    
    md_content += "\n\n## Key Insights\n"
    success_rate = (df[df['Status'] == 'Success']['RunCount'].sum() / df['RunCount'].sum()) * 100 if not df.empty else 0
    md_content += f"- **Overall Success Rate:** {success_rate:.1f}%\n"
    md_content += f"- **Total Rows Processed (7 Days):** {df['TotalRows'].sum():,}\n"

    # Save to file
    with open("ELT_Report.md", "w") as f:
        f.write(md_content)
    
    print("Report saved as ELT_Report.md")

if __name__ == "__main__":
    generate_markdown_report()

```

---

###Step 2: Running the Report1. In your terminal, run: `python elt_report.py`
2. In VS Code, you will see a new file appear called `ELT_Report.md`.
3. **To view it properly:** Click on the `ELT_Report.md` file, then press **Ctrl + Shift + V** to open the Markdown Preview.

###Why this mattersIn a professional setting, nobody wants to look at raw SQL tables. By generating a report:

* **Visibility:** You can see if a specific day had multiple failures.
* **Volume Tracking:** You can monitor if the `TotalRows` is growing unexpectedly, which might indicate a bug or a sudden spike in business.
* **Documentation:** You have an automated way to prove your pipeline is working.

---

###You now have a complete Data Engineering ecosystem:1. **Extract & Load** (`elt_load.py`)
2. **Transform & Batching** (`SSMS Stored Procedures`)
3. **Orchestration & Audit** (`elt_audit.py`)
4. **Maintenance** (`elt_maintenance.py`)
5. **Reporting** (`elt_report.py`)

**What would you like to explore next?** * We could look at **Data Quality Checks** (e.g., verifying that `TotalAmount` is never negative before loading).

* Or we could move toward **Data Visualization** (connecting Excel or Power BI to your `DWH.Fact_Sales` table).