IF OBJECT_ID(N'dbo.SyncProgress', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.SyncProgress
    (
        sync_name nvarchar(128) NOT NULL,
        table_name sysname NOT NULL,
        last_ccdr_id char(36) NULL,
        rows_processed bigint NOT NULL
            CONSTRAINT DF_SyncProgress_rows_processed DEFAULT 0,
        source_total_rows bigint NULL,
        status nvarchar(32) NOT NULL,
        started_at datetime2 NOT NULL
            CONSTRAINT DF_SyncProgress_started_at DEFAULT SYSUTCDATETIME(),
        updated_at datetime2 NOT NULL
            CONSTRAINT DF_SyncProgress_updated_at DEFAULT SYSUTCDATETIME(),
        completed_at datetime2 NULL,
        last_error nvarchar(max) NULL,
        CONSTRAINT PK_SyncProgress PRIMARY KEY (sync_name, table_name)
    );
END;

IF COL_LENGTH(N'dbo.SyncProgress', N'source_total_rows') IS NULL
BEGIN
    ALTER TABLE dbo.SyncProgress
    ADD source_total_rows bigint NULL;
END;