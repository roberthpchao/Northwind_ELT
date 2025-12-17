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