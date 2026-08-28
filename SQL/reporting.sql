SELECT
    'section2' AS table_name, COUNT_BIG(*) AS rows FROM dbo.section2 WITH (NOLOCK)
UNION ALL
SELECT 'section_meta2', COUNT_BIG(*) FROM dbo.section_meta2 WITH (NOLOCK)
UNION ALL
SELECT 'section_centera2', COUNT_BIG(*) FROM dbo.section_centera2 WITH (NOLOCK)
UNION ALL
SELECT 'section_cdr_media2', COUNT_BIG(*) FROM dbo.section_cdr_media2 WITH (NOLOCK);

DECLARE @SyncName nvarchar(128) = N'VFC_AMR_RP_to_VF_AMR_RP1';

WITH ProgressData AS
(
    SELECT
        sync_name,
        table_name,
        status,
        last_ccdr_id,
        rows_processed,
        source_total_rows,
        started_at,
        updated_at,
        completed_at,
        DATEDIFF(SECOND, started_at, ISNULL(completed_at, SYSUTCDATETIME())) AS elapsed_seconds,
        last_error
    FROM dbo.SyncProgress
    WHERE sync_name = @SyncName
),
EtaData AS
(
    SELECT
        *,
        CAST(
            CASE
                WHEN source_total_rows IS NULL OR source_total_rows = 0 THEN NULL
                ELSE (rows_processed * 100.0) / source_total_rows
            END
            AS decimal(6,2)
        ) AS percent_complete,
        CASE
            WHEN status = 'completed' THEN 0
            WHEN rows_processed <= 0 THEN NULL
            WHEN source_total_rows IS NULL OR source_total_rows <= rows_processed THEN NULL
            ELSE CAST(
                (elapsed_seconds * 1.0 / rows_processed) * (source_total_rows - rows_processed)
                AS bigint
            )
        END AS eta_seconds
    FROM ProgressData
)
SELECT
    sync_name,
    table_name,
    status,
    last_ccdr_id,
    rows_processed,
    source_total_rows,
    percent_complete,
    started_at,
    updated_at,
    completed_at,
    CONCAT(
        elapsed_seconds / 86400, ' days ',
        RIGHT('00' + CAST((elapsed_seconds % 86400) / 3600 AS varchar(2)), 2), ' hours ',
        RIGHT('00' + CAST((elapsed_seconds % 3600) / 60 AS varchar(2)), 2), ' minutes'
    ) AS elapsed_time,
    CASE
        WHEN eta_seconds IS NULL THEN NULL
        ELSE CONCAT(
            eta_seconds / 86400, ' days ',
            RIGHT('00' + CAST((eta_seconds % 86400) / 3600 AS varchar(2)), 2), ' hours ',
            RIGHT('00' + CAST((eta_seconds % 3600) / 60 AS varchar(2)), 2), ' minutes'
        )
    END AS eta_to_complete,
    last_error
FROM EtaData
ORDER BY
    CASE table_name
        WHEN 'dbo.section2' THEN 1
        WHEN 'dbo.section_meta2' THEN 2
        WHEN 'dbo.section_centera2' THEN 3
        WHEN 'dbo.section_cdr_media2' THEN 4
        ELSE 99
    END;

--    USE [VF_AMR_RP1];
--GO

--BEGIN TRANSACTION;

--TRUNCATE TABLE dbo.section_cdr_media2;
--TRUNCATE TABLE dbo.section_centera2;
--TRUNCATE TABLE dbo.section_meta2;
--TRUNCATE TABLE dbo.section2;

--DELETE
--FROM dbo.SyncProgress
--WHERE sync_name = N'VFC_AMR_RP_to_VF_AMR_RP1';

--COMMIT TRANSACTION;
--GO