-- =============================================================================
-- Insurance Data Platform — Azure SQL Database Export
-- Database: policyadmin-db  |  Schema: dbo
-- Exported from live INFORMATION_SCHEMA on 2026-09-17.
-- Redeploy on any fresh Azure SQL Database by running this file top to bottom.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TABLE: Customer
-- Loaded by: PL_Ingest_CustomerDB (SQL incremental, watermark = LastUpdated)
-- -----------------------------------------------------------------------------
CREATE TABLE dbo.Customer (
    CustomerID     VARCHAR(20)   NOT NULL PRIMARY KEY,
    FirstName      VARCHAR(100)  NULL,
    LastName       VARCHAR(100)  NULL,
    DOB            DATE          NULL,
    Gender         VARCHAR(20)   NULL,
    City           VARCHAR(100)  NULL,
    State          VARCHAR(100)  NULL,
    Phone          VARCHAR(30)   NULL,
    Email          VARCHAR(200)  NULL,
    KYCStatus      VARCHAR(30)   NULL,
    CustomerSince  DATE          NULL,
    LastUpdated    DATETIME2     NULL
);
GO

-- -----------------------------------------------------------------------------
-- TABLE: Agent
-- Loaded by: PL_Ingest_AgentMasterDB (SQL full snapshot, no watermark)
-- -----------------------------------------------------------------------------
CREATE TABLE dbo.Agent (
    AgentID    VARCHAR(20)   NOT NULL PRIMARY KEY,
    AgentName  VARCHAR(100)  NULL,
    Branch     VARCHAR(100)  NULL,
    JoinDate   DATE          NULL,
    Status     VARCHAR(20)   NULL
);
GO

-- -----------------------------------------------------------------------------
-- TABLE: Policy
-- Loaded by: PL_Ingest_PolicyAdminDB (SQL incremental, watermark = LastUpdated)
-- -----------------------------------------------------------------------------
CREATE TABLE dbo.Policy (
    PolicyID          VARCHAR(20)     NOT NULL PRIMARY KEY,
    CustomerID        VARCHAR(20)     NULL,
    AgentID           VARCHAR(20)     NULL,
    PolicyType        VARCHAR(50)     NULL,
    PremiumAmount     DECIMAL(18,2)   NULL,
    SumAssured        DECIMAL(18,2)   NULL,
    StartDate         DATE            NULL,
    EndDate           DATE            NULL,
    Status            VARCHAR(30)     NULL,
    Branch            VARCHAR(100)    NULL,
    PaymentFrequency  VARCHAR(30)     NULL,
    LastUpdated       DATETIME2       NULL,
    CONSTRAINT FK_Policy_Customer FOREIGN KEY (CustomerID) REFERENCES dbo.Customer(CustomerID),
    CONSTRAINT FK_Policy_Agent    FOREIGN KEY (AgentID)    REFERENCES dbo.Agent(AgentID)
);
GO

-- -----------------------------------------------------------------------------
-- TABLE: SourceControlConfig
-- Drives PL_Master_Ingestion's ForEach_ActiveSources loop
-- -----------------------------------------------------------------------------
CREATE TABLE dbo.SourceControlConfig (
    SourceName        VARCHAR(100)  NOT NULL PRIMARY KEY,
    SourceType        VARCHAR(20)   NOT NULL,   -- 'SQL' or 'FILE'
    SourceObject      VARCHAR(100)  NULL,
    TargetPath        VARCHAR(200)  NOT NULL,
    WatermarkColumn   VARCHAR(100)  NULL,       -- NULL = full load, no incremental
    FileNamePattern   VARCHAR(200)  NULL,       -- used only for SourceType = 'FILE'
    Status            VARCHAR(20)   NOT NULL    -- 'ACTIVE' / 'INACTIVE'
);
GO

-- Seed data reflecting the actual build:
INSERT INTO dbo.SourceControlConfig (SourceName, SourceType, SourceObject, TargetPath, WatermarkColumn, FileNamePattern, Status)
VALUES
    ('Policy',   'SQL',  'Policy',  'insurance/policy',   'LastUpdated', NULL,          'ACTIVE'),
    ('Customer', 'SQL',  'Customer','insurance/customer', 'LastUpdated', NULL,          'ACTIVE'),
    ('Agent',    'SQL',  'Agent',   'insurance/agent',     NULL,          NULL,          'ACTIVE'),
    ('Claims',   'FILE', NULL,      'insurance/claims',    NULL,          'Claims_*.csv','ACTIVE');
GO

-- -----------------------------------------------------------------------------
-- TABLE: WatermarkTable
-- Tracks incremental load position per source; updated by usp_UpdateWatermark
-- -----------------------------------------------------------------------------
CREATE TABLE dbo.WatermarkTable (
    SourceName           VARCHAR(100)  NOT NULL PRIMARY KEY,
    LastWatermarkValue   DATETIME2     NOT NULL,
    LastRunStatus         VARCHAR(20)   NOT NULL,
    LastRunTime            DATETIME2     NOT NULL
);
GO

-- -----------------------------------------------------------------------------
-- TABLE: AuditLog
-- Written by every child pipeline via usp_LogAudit after each run
-- -----------------------------------------------------------------------------
CREATE TABLE dbo.AuditLog (
    AuditLogID    INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PipelineName  VARCHAR(200)      NOT NULL,
    RunDate       DATETIME2         NOT NULL,
    Status        VARCHAR(50)       NOT NULL,
    Message       VARCHAR(1000)     NULL,
    rowsCopied    BIGINT            NULL
);
GO

-- =============================================================================
-- STORED PROCEDURES
-- =============================================================================

CREATE PROCEDURE dbo.usp_LogAudit
    @PipelineName VARCHAR(200),
    @Status VARCHAR(50),
    @Message VARCHAR(1000) = NULL,
    @rowsCopied BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.AuditLog
    (
        PipelineName,
        RunDate,
        Status,
        Message,
        rowsCopied
    )
    VALUES
    (
        @PipelineName,
        SYSDATETIME(),
        @Status,
        @Message,
        @rowsCopied
    );
END;
GO

CREATE PROCEDURE dbo.usp_UpdateWatermark
    @SourceName VARCHAR(100),
    @NewWatermark DATETIME2,
    @RunStatus VARCHAR(20) = 'SUCCESS'
AS
BEGIN
    UPDATE dbo.WatermarkTable
    SET
        LastWatermarkValue = @NewWatermark,
        LastRunStatus = @RunStatus,
        LastRunTime = SYSUTCDATETIME()
    WHERE SourceName = @SourceName;
END;
GO

-- =============================================================================
-- NOTES
-- - FK constraints on Policy assume Customer/Agent are loaded first — matches
--   the actual pipeline order (Customer, Agent ingested before/independently
--   of Policy in PL_Master_Ingestion's ForEach loop).
-- - WatermarkTable needs one seed row per incremental source (Policy, Customer)
--   before the first incremental run, e.g.:
--   INSERT INTO dbo.WatermarkTable VALUES ('Policy', '1900-01-01', 'SUCCESS', SYSUTCDATETIME());
--   INSERT INTO dbo.WatermarkTable VALUES ('Customer', '1900-01-01', 'SUCCESS', SYSUTCDATETIME());
-- - Claims (file-based) has no WatermarkTable entry — file arrival is the trigger.
-- =============================================================================
