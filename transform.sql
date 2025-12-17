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