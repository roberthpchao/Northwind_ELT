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