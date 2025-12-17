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