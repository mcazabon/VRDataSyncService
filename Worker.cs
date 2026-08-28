using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace VRDataSyncService;

public sealed class Worker : BackgroundService
{
    private const string ProgressTableDdl = """
CREATE TABLE dbo.SyncProgress
(
    sync_name nvarchar(128) NOT NULL,
    table_name sysname NOT NULL,
    last_ccdr_id char(36) NULL,
    rows_processed bigint NOT NULL CONSTRAINT DF_SyncProgress_rows_processed DEFAULT 0,
    source_total_rows bigint NULL,
    status nvarchar(32) NOT NULL,
    started_at datetime2 NOT NULL CONSTRAINT DF_SyncProgress_started_at DEFAULT SYSUTCDATETIME(),
    updated_at datetime2 NOT NULL CONSTRAINT DF_SyncProgress_updated_at DEFAULT SYSUTCDATETIME(),
    completed_at datetime2 NULL,
    last_error nvarchar(max) NULL,
    CONSTRAINT PK_SyncProgress PRIMARY KEY (sync_name, table_name)
);
""";

    private readonly ILogger<Worker> _logger;
    private readonly SyncOptions _options;
    private readonly IHostApplicationLifetime _lifetime;

    public Worker(ILogger<Worker> logger, IOptions<SyncOptions> options, IHostApplicationLifetime lifetime)
    {
        _logger = logger;
        _options = options.Value;
        _lifetime = lifetime;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            ValidateOptions(_options);

            var batchSize = Math.Max(1, _options.BatchSize);
            var syncName = _options.SyncName.Trim();
            var maxSection2RowsToTransfer = _options.MaxSection2RowsToTransfer;

            await using var sourceConnection = new SqlConnection(_options.SourceConnectionString);
            await using var destinationConnection = new SqlConnection(_options.DestinationConnectionString);

            await sourceConnection.OpenAsync(stoppingToken);
            await destinationConnection.OpenAsync(stoppingToken);

            await EnsureProgressTableAsync(destinationConnection, stoppingToken);

            var maxSection2CcdrId = await GetMaxSection2CcdrIdAsync(sourceConnection, maxSection2RowsToTransfer, stoppingToken);
            if (!string.IsNullOrWhiteSpace(maxSection2CcdrId))
            {
                _logger.LogInformation(
                    "Testing limit enabled: syncing up to {MaxSection2RowsToTransfer} section2 rows ending at CCDR_ID {MaxSection2CcdrId}.",
                    maxSection2RowsToTransfer,
                    maxSection2CcdrId);
            }

            await SyncSection2Async(sourceConnection, destinationConnection, syncName, batchSize, maxSection2CcdrId, stoppingToken);
            await SyncSectionMeta2Async(sourceConnection, destinationConnection, syncName, batchSize, maxSection2CcdrId, stoppingToken);
            await SyncSectionCentera2Async(sourceConnection, destinationConnection, syncName, batchSize, maxSection2CcdrId, stoppingToken);
            await SyncSectionCdrMedia2Async(sourceConnection, destinationConnection, syncName, batchSize, maxSection2CcdrId, stoppingToken);

            _logger.LogInformation("Sync completed for {SyncName}.", syncName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Synchronization failed.");
            throw;
        }
        finally
        {
            _lifetime.StopApplication();
        }
    }

    private async Task SyncSection2Async(
        SqlConnection source,
        SqlConnection destination,
        string syncName,
        int batchSize,
        string? maxSection2CcdrId,
        CancellationToken cancellationToken)
    {
        const string tableName = "dbo.section2";
        const string keyColumn = "CCDR_ID";

        var sourceColumns = await GetColumnsAsync(source, tableName, cancellationToken);
        var destinationColumns = await GetColumnsAsync(destination, tableName, cancellationToken);
        var commonColumns = GetCommonColumns(sourceColumns, destinationColumns);

        if (!commonColumns.Any(c => c.Equals(keyColumn, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException($"{tableName} must contain {keyColumn} in source and destination.");
        }

        await SyncInBatchesAsync(
            destination,
            syncName,
            tableName,
            cancellationToken,
            fetchSourceTotalRowsAsync: ct => CountRowsByCcdrIdRangeAsync(source, tableName, keyColumn, maxSection2CcdrId, ct),
            fetchBoundaryAsync: (lastCcdrId, ct) => GetParentBoundaryAsync(source, lastCcdrId, batchSize, maxSection2CcdrId, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadSection2RowsAsync(source, commonColumns, lastCcdrId, batchSize, maxSection2CcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => MergeBatchAsync(destination, tx, tableName, commonColumns, new[] { keyColumn }, rows, ct));
    }

    private async Task SyncSectionMeta2Async(
        SqlConnection source,
        SqlConnection destination,
        string syncName,
        int batchSize,
        string? maxSection2CcdrId,
        CancellationToken cancellationToken)
    {
        const string tableName = "dbo.section_meta2";

        var sourceColumns = await GetColumnsAsync(source, tableName, cancellationToken);
        var destinationColumns = await GetColumnsAsync(destination, tableName, cancellationToken);
        var commonColumns = GetCommonColumns(sourceColumns, destinationColumns);

        var mergeKeys = SelectFirstAvailableKeySet(
            commonColumns,
            new[] { "ccdr_id", "section_start_time" },
            new[] { "ccdr_id" });

        if (mergeKeys.Count == 0)
        {
            throw new InvalidOperationException($"Unable to determine a merge key for {tableName}.");
        }

        await SyncInBatchesAsync(
            destination,
            syncName,
            tableName,
            cancellationToken,
            fetchSourceTotalRowsAsync: ct => CountRowsByCcdrIdRangeAsync(source, tableName, "ccdr_id", maxSection2CcdrId, ct),
            fetchBoundaryAsync: (lastCcdrId, ct) => GetParentBoundaryAsync(source, lastCcdrId, batchSize, maxSection2CcdrId, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadTableRangeAsync(source, tableName, commonColumns, "ccdr_id", lastCcdrId, maxCcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => MergeBatchAsync(destination, tx, tableName, commonColumns, mergeKeys, rows, ct));
    }

    private async Task SyncSectionCentera2Async(
        SqlConnection source,
        SqlConnection destination,
        string syncName,
        int batchSize,
        string? maxSection2CcdrId,
        CancellationToken cancellationToken)
    {
        const string tableName = "dbo.section_centera2";

        var destinationColumns = await GetColumnsAsync(destination, tableName, cancellationToken);
        var requiredDestinationColumns = new[]
        {
            "ccdr_id", "extension", "clip_id", "moved_to_section_centera2", "ts_insert1", "section_start_time"
        };

        EnsureColumnsExist(tableName, destinationColumns, requiredDestinationColumns);

        await SyncInBatchesAsync(
            destination,
            syncName,
            tableName,
            cancellationToken,
            fetchSourceTotalRowsAsync: ct => CountRowsByCcdrIdRangeAsync(source, tableName, "ccdr_id", maxSection2CcdrId, ct),
            fetchBoundaryAsync: (lastCcdrId, ct) => GetParentBoundaryAsync(source, lastCcdrId, batchSize, maxSection2CcdrId, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadSectionCenteraRowsAsync(source, lastCcdrId, maxCcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => InsertIfNotExistsAsync(
                destination,
                tx,
                tableName,
                requiredDestinationColumns,
                requiredDestinationColumns,
                rows,
                ct));
    }

    private async Task SyncSectionCdrMedia2Async(
        SqlConnection source,
        SqlConnection destination,
        string syncName,
        int batchSize,
        string? maxSection2CcdrId,
        CancellationToken cancellationToken)
    {
        const string tableName = "dbo.section_cdr_media2";

        var sourceColumns = await GetColumnsAsync(source, tableName, cancellationToken);
        var destinationColumns = await GetColumnsAsync(destination, tableName, cancellationToken);
        var commonColumns = GetCommonColumns(sourceColumns, destinationColumns)
            .Where(c => !c.Equals("section_start_time", StringComparison.OrdinalIgnoreCase))
            .ToList();

        commonColumns.Add("section_start_time");

        EnsureColumnsExist(tableName, destinationColumns, commonColumns);

        var mergeKeys = SelectFirstAvailableKeySet(
            commonColumns,
            new[] { "id" },
            new[] { "ccdr_id", "section_start_time" });

        await SyncInBatchesAsync(
            destination,
            syncName,
            tableName,
            cancellationToken,
            fetchSourceTotalRowsAsync: ct => CountRowsByCcdrIdRangeAsync(source, tableName, "ccdr_id", maxSection2CcdrId, ct),
            fetchBoundaryAsync: (lastCcdrId, ct) => GetParentBoundaryAsync(source, lastCcdrId, batchSize, maxSection2CcdrId, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadSectionCdrMediaRowsAsync(source, commonColumns, lastCcdrId, maxCcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => mergeKeys.Count > 0
                ? MergeBatchAsync(destination, tx, tableName, commonColumns, mergeKeys, rows, ct)
                : InsertIfNotExistsAsync(destination, tx, tableName, commonColumns, commonColumns, rows, ct));
    }

    private async Task SyncInBatchesAsync(
        SqlConnection destination,
        string syncName,
        string tableName,
        CancellationToken cancellationToken,
        Func<CancellationToken, Task<long>> fetchSourceTotalRowsAsync,
        Func<string?, CancellationToken, Task<(bool hasRows, string? maxCcdrId)>> fetchBoundaryAsync,
        Func<string?, string?, CancellationToken, Task<DataTable>> fetchRowsAsync,
        Func<DataTable, SqlTransaction, CancellationToken, Task> writeBatchAsync)
    {
        var lastCcdrId = await GetLastCcdrIdAsync(destination, syncName, tableName, cancellationToken);

        try
        {
            var sourceTotalRows = await fetchSourceTotalRowsAsync(cancellationToken);
            await UpsertProgressAsync(
                destination,
                transaction: null,
                syncName,
                tableName,
                lastCcdrId,
                rowsIncrement: 0,
                sourceTotalRows,
                status: "running",
                completedAtUtc: null,
                lastError: null,
                cancellationToken);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var (hasRows, maxCcdrId) = await fetchBoundaryAsync(lastCcdrId, cancellationToken);
                if (!hasRows || string.IsNullOrWhiteSpace(maxCcdrId))
                {
                    break;
                }

                var rows = await fetchRowsAsync(lastCcdrId, maxCcdrId, cancellationToken);

                await using var tx = await destination.BeginTransactionAsync(cancellationToken);
                await writeBatchAsync(rows, (SqlTransaction)tx, cancellationToken);
                await UpsertProgressAsync(
                    destination,
                    (SqlTransaction)tx,
                    syncName,
                    tableName,
                    maxCcdrId,
                    rows.Rows.Count,
                    sourceTotalRows: null,
                    status: "running",
                    completedAtUtc: null,
                    lastError: null,
                    cancellationToken);
                await tx.CommitAsync(cancellationToken);

                lastCcdrId = maxCcdrId;
                _logger.LogInformation(
                    "Synced batch for {TableName} through CCDR_ID {LastCcdrId}. Rows: {RowCount}",
                    tableName,
                    lastCcdrId,
                    rows.Rows.Count);
            }

            await UpsertProgressAsync(
                destination,
                transaction: null,
                syncName,
                tableName,
                lastCcdrId,
                rowsIncrement: 0,
                sourceTotalRows: null,
                status: "completed",
                completedAtUtc: DateTime.UtcNow,
                lastError: null,
                cancellationToken);
        }
        catch (Exception ex)
        {
            await UpsertProgressAsync(
                destination,
                transaction: null,
                syncName,
                tableName,
                lastCcdrId,
                rowsIncrement: 0,
                sourceTotalRows: null,
                status: "failed",
                completedAtUtc: null,
                lastError: ex.ToString(),
                cancellationToken);

            _logger.LogError(ex, "Failed syncing {TableName}.", tableName);
            throw;
        }
    }

    private static async Task EnsureProgressTableAsync(SqlConnection destination, CancellationToken cancellationToken)
    {
        var commandText = $"""
IF OBJECT_ID(N'dbo.SyncProgress', N'U') IS NULL
BEGIN
    {ProgressTableDdl}
END

IF COL_LENGTH(N'dbo.SyncProgress', N'source_total_rows') IS NULL
BEGIN
    ALTER TABLE dbo.SyncProgress ADD source_total_rows bigint NULL;
END
""";

        await using var command = new SqlCommand(commandText, destination);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<(bool hasRows, string? maxCcdrId)> GetParentBoundaryAsync(
        SqlConnection source,
        string? lastCcdrId,
        int batchSize,
        string? maxSection2CcdrId,
        CancellationToken cancellationToken)
    {
        const string sql = """
SELECT
    MAX(batch.CCDR_ID) AS max_ccdr_id,
    COUNT(1) AS row_count
FROM
(
    SELECT TOP (@batchSize) s.CCDR_ID
    FROM dbo.section2 AS s
    WHERE (@lastCcdrId IS NULL OR s.CCDR_ID > @lastCcdrId)
      AND (@maxSection2CcdrId IS NULL OR s.CCDR_ID <= @maxSection2CcdrId)
    ORDER BY s.CCDR_ID
) AS batch;
""";

        await using var command = new SqlCommand(sql, source);
        command.Parameters.AddWithValue("@batchSize", batchSize);
        command.Parameters.AddWithValue("@lastCcdrId", (object?)lastCcdrId ?? DBNull.Value);
        command.Parameters.AddWithValue("@maxSection2CcdrId", (object?)maxSection2CcdrId ?? DBNull.Value);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (false, null);
        }

        var count = reader.GetInt32(reader.GetOrdinal("row_count"));
        var max = reader.IsDBNull(reader.GetOrdinal("max_ccdr_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("max_ccdr_id"));

        return (count > 0, max);
    }

    private static async Task<DataTable> ReadSection2RowsAsync(
        SqlConnection source,
        IReadOnlyList<string> columns,
        string? lastCcdrId,
        int batchSize,
        string? maxSection2CcdrId,
        CancellationToken cancellationToken)
    {
        var selectColumns = BuildColumnList(columns);
        var sql = $"""
SELECT TOP (@batchSize) {selectColumns}
FROM dbo.section2 AS s
WHERE (@lastCcdrId IS NULL OR s.CCDR_ID > @lastCcdrId)
  AND (@maxSection2CcdrId IS NULL OR s.CCDR_ID <= @maxSection2CcdrId)
ORDER BY s.CCDR_ID;
""";

        await using var command = new SqlCommand(sql, source);
        command.Parameters.AddWithValue("@batchSize", batchSize);
        command.Parameters.AddWithValue("@lastCcdrId", (object?)lastCcdrId ?? DBNull.Value);
        command.Parameters.AddWithValue("@maxSection2CcdrId", (object?)maxSection2CcdrId ?? DBNull.Value);

        return await ReadDataTableAsync(command, cancellationToken);
    }

    private static async Task<DataTable> ReadTableRangeAsync(
        SqlConnection source,
        string tableName,
        IReadOnlyList<string> columns,
        string ccdrColumn,
        string? lastCcdrId,
        string? maxCcdrId,
        CancellationToken cancellationToken)
    {
        var selectColumns = string.Join(", ", columns.Select(c => $"t.{QuoteIdentifier(c)}"));
        var sql = $"""
SELECT {selectColumns}
FROM {tableName} AS t
WHERE (@lastCcdrId IS NULL OR t.{QuoteIdentifier(ccdrColumn)} > @lastCcdrId)
  AND t.{QuoteIdentifier(ccdrColumn)} <= @maxCcdrId
ORDER BY t.{QuoteIdentifier(ccdrColumn)};
""";

        await using var command = new SqlCommand(sql, source);
        command.Parameters.AddWithValue("@lastCcdrId", (object?)lastCcdrId ?? DBNull.Value);
        command.Parameters.AddWithValue("@maxCcdrId", maxCcdrId ?? throw new InvalidOperationException("maxCcdrId is required."));

        return await ReadDataTableAsync(command, cancellationToken);
    }

    private static async Task<DataTable> ReadSectionCenteraRowsAsync(
        SqlConnection source,
        string? lastCcdrId,
        string? maxCcdrId,
        CancellationToken cancellationToken)
    {
        const string sql = """
SELECT
    c.ccdr_id,
    c.extension,
    c.clip_id,
    c.moved_to_section_centera2,
    c.ts_insert1,
    s.Start_Time AS section_start_time
FROM dbo.section_centera2 AS c
LEFT JOIN dbo.section2 AS s
    ON s.CCDR_ID = c.ccdr_id
WHERE (@lastCcdrId IS NULL OR c.ccdr_id > @lastCcdrId)
  AND c.ccdr_id <= @maxCcdrId
ORDER BY c.ccdr_id;
""";

        await using var command = new SqlCommand(sql, source);
        command.Parameters.AddWithValue("@lastCcdrId", (object?)lastCcdrId ?? DBNull.Value);
        command.Parameters.AddWithValue("@maxCcdrId", maxCcdrId ?? throw new InvalidOperationException("maxCcdrId is required."));

        return await ReadDataTableAsync(command, cancellationToken);
    }

    private static async Task<DataTable> ReadSectionCdrMediaRowsAsync(
        SqlConnection source,
        IReadOnlyList<string> columns,
        string? lastCcdrId,
        string? maxCcdrId,
        CancellationToken cancellationToken)
    {
        var projection = columns.Select(c =>
            c.Equals("section_start_time", StringComparison.OrdinalIgnoreCase)
                ? "s.Start_Time AS section_start_time"
                : $"m.{QuoteIdentifier(c)}");

        var sql = $"""
SELECT {string.Join(", ", projection)}
FROM dbo.section_cdr_media2 AS m
LEFT JOIN dbo.section2 AS s
    ON s.CCDR_ID = m.ccdr_id
WHERE (@lastCcdrId IS NULL OR m.ccdr_id > @lastCcdrId)
  AND m.ccdr_id <= @maxCcdrId
ORDER BY m.ccdr_id;
""";

        await using var command = new SqlCommand(sql, source);
        command.Parameters.AddWithValue("@lastCcdrId", (object?)lastCcdrId ?? DBNull.Value);
        command.Parameters.AddWithValue("@maxCcdrId", maxCcdrId ?? throw new InvalidOperationException("maxCcdrId is required."));

        return await ReadDataTableAsync(command, cancellationToken);
    }

    private static async Task MergeBatchAsync(
        SqlConnection destination,
        SqlTransaction transaction,
        string tableName,
        IReadOnlyList<string> columns,
        IReadOnlyList<string> keyColumns,
        DataTable rows,
        CancellationToken cancellationToken)
    {
        if (rows.Rows.Count == 0)
        {
            return;
        }

        var stageTable = $"#SyncStage_{Guid.NewGuid():N}";
        var columnList = BuildColumnList(columns);

        try
        {
            await ExecuteNonQueryAsync(
                destination,
                transaction,
                $"SELECT TOP (0) {columnList} INTO {stageTable} FROM {tableName};",
                cancellationToken);

            await BulkCopyAsync(destination, transaction, stageTable, columns, rows, cancellationToken);

            var keyPredicate = string.Join(
                " AND ",
                keyColumns.Select(c => $"target.{QuoteIdentifier(c)} = source.{QuoteIdentifier(c)}"));

            var updateColumns = columns.Where(c => keyColumns.All(k => !k.Equals(c, StringComparison.OrdinalIgnoreCase))).ToList();
            var updateClause = updateColumns.Count == 0
                ? string.Empty
                : "WHEN MATCHED THEN UPDATE SET " + string.Join(", ", updateColumns.Select(c => $"target.{QuoteIdentifier(c)} = source.{QuoteIdentifier(c)}"));

            var insertColumns = BuildColumnList(columns);
            var insertValues = string.Join(", ", columns.Select(c => $"source.{QuoteIdentifier(c)}"));

            var mergeSql = $"""
MERGE {tableName} AS target
USING {stageTable} AS source
ON {keyPredicate}
{updateClause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insertColumns})
    VALUES ({insertValues});
""";

            await ExecuteNonQueryAsync(destination, transaction, mergeSql, cancellationToken);
        }
        finally
        {
            await ExecuteNonQueryAsync(destination, transaction, $"DROP TABLE IF EXISTS {stageTable};", cancellationToken);
        }
    }

    private static async Task InsertIfNotExistsAsync(
        SqlConnection destination,
        SqlTransaction transaction,
        string tableName,
        IReadOnlyList<string> columns,
        IReadOnlyList<string> dedupeColumns,
        DataTable rows,
        CancellationToken cancellationToken)
    {
        if (rows.Rows.Count == 0)
        {
            return;
        }

        var stageTable = $"#SyncStage_{Guid.NewGuid():N}";
        var columnList = BuildColumnList(columns);

        try
        {
            await ExecuteNonQueryAsync(
                destination,
                transaction,
                $"SELECT TOP (0) {columnList} INTO {stageTable} FROM {tableName};",
                cancellationToken);

            await BulkCopyAsync(destination, transaction, stageTable, columns, rows, cancellationToken);

            var nullSafePredicate = string.Join(
                " AND ",
                dedupeColumns.Select(c =>
                    $"((t.{QuoteIdentifier(c)} = s.{QuoteIdentifier(c)}) OR (t.{QuoteIdentifier(c)} IS NULL AND s.{QuoteIdentifier(c)} IS NULL))"));

            var insertSql = $"""
INSERT INTO {tableName} ({columnList})
SELECT {string.Join(", ", columns.Select(c => $"s.{QuoteIdentifier(c)}"))}
FROM {stageTable} AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM {tableName} AS t
    WHERE {nullSafePredicate}
);
""";

            await ExecuteNonQueryAsync(destination, transaction, insertSql, cancellationToken);
        }
        finally
        {
            await ExecuteNonQueryAsync(destination, transaction, $"DROP TABLE IF EXISTS {stageTable};", cancellationToken);
        }
    }

    private static async Task BulkCopyAsync(
        SqlConnection destination,
        SqlTransaction transaction,
        string stageTable,
        IReadOnlyList<string> columns,
        DataTable rows,
        CancellationToken cancellationToken)
    {
        using var bulkCopy = new SqlBulkCopy(destination, SqlBulkCopyOptions.CheckConstraints, transaction)
        {
            DestinationTableName = stageTable,
            BatchSize = rows.Rows.Count,
            BulkCopyTimeout = 0
        };

        foreach (var column in columns)
        {
            bulkCopy.ColumnMappings.Add(column, column);
        }

        await bulkCopy.WriteToServerAsync(rows, cancellationToken);
    }

    private static async Task UpsertProgressAsync(
        SqlConnection destination,
        SqlTransaction? transaction,
        string syncName,
        string tableName,
        string? lastCcdrId,
        long rowsIncrement,
        long? sourceTotalRows,
        string status,
        DateTime? completedAtUtc,
        string? lastError,
        CancellationToken cancellationToken)
    {
        const string sql = """
MERGE dbo.SyncProgress AS target
USING (SELECT @syncName AS sync_name, @tableName AS table_name) AS source
    ON target.sync_name = source.sync_name AND target.table_name = source.table_name
WHEN MATCHED THEN
    UPDATE SET
        last_ccdr_id = @lastCcdrId,
        rows_processed = target.rows_processed + @rowsIncrement,
        source_total_rows = COALESCE(@sourceTotalRows, target.source_total_rows),
        status = @status,
        updated_at = SYSUTCDATETIME(),
        completed_at = @completedAt,
        last_error = @lastError
WHEN NOT MATCHED THEN
    INSERT (sync_name, table_name, last_ccdr_id, rows_processed, source_total_rows, status, completed_at, last_error)
    VALUES (@syncName, @tableName, @lastCcdrId, @rowsIncrement, @sourceTotalRows, @status, @completedAt, @lastError);
""";

        await using var command = new SqlCommand(sql, destination, transaction);
        command.Parameters.AddWithValue("@syncName", syncName);
        command.Parameters.AddWithValue("@tableName", tableName);
        command.Parameters.AddWithValue("@lastCcdrId", (object?)lastCcdrId ?? DBNull.Value);
        command.Parameters.AddWithValue("@rowsIncrement", rowsIncrement);
        command.Parameters.AddWithValue("@sourceTotalRows", (object?)sourceTotalRows ?? DBNull.Value);
        command.Parameters.AddWithValue("@status", status);
        command.Parameters.AddWithValue("@completedAt", (object?)completedAtUtc ?? DBNull.Value);
        command.Parameters.AddWithValue("@lastError", (object?)lastError ?? DBNull.Value);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> GetLastCcdrIdAsync(
        SqlConnection destination,
        string syncName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = """
SELECT last_ccdr_id
FROM dbo.SyncProgress
WHERE sync_name = @syncName
  AND table_name = @tableName;
""";

        await using var command = new SqlCommand(sql, destination);
        command.Parameters.AddWithValue("@syncName", syncName);
        command.Parameters.AddWithValue("@tableName", tableName);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value == DBNull.Value || value is null ? null : Convert.ToString(value);
    }

    private static async Task<string?> GetMaxSection2CcdrIdAsync(
        SqlConnection source,
        long? maxSection2RowsToTransfer,
        CancellationToken cancellationToken)
    {
        if (maxSection2RowsToTransfer is null)
        {
            return null;
        }

        const string sql = """
SELECT MAX(batch.CCDR_ID)
FROM
(
    SELECT TOP (@maxSection2RowsToTransfer) s.CCDR_ID
    FROM dbo.section2 AS s
    ORDER BY s.CCDR_ID
) AS batch;
""";

        await using var command = new SqlCommand(sql, source);
        command.Parameters.AddWithValue("@maxSection2RowsToTransfer", maxSection2RowsToTransfer.Value);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value == DBNull.Value || value is null ? null : Convert.ToString(value);
    }

    private static async Task<long> CountRowsByCcdrIdRangeAsync(
        SqlConnection source,
        string tableName,
        string ccdrColumn,
        string? maxSection2CcdrId,
        CancellationToken cancellationToken)
    {
        var sql = $"""
SELECT COUNT_BIG(1)
FROM {tableName}
WHERE (@maxSection2CcdrId IS NULL OR {QuoteIdentifier(ccdrColumn)} <= @maxSection2CcdrId);
""";

        await using var command = new SqlCommand(sql, source);
        command.Parameters.AddWithValue("@maxSection2CcdrId", (object?)maxSection2CcdrId ?? DBNull.Value);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null || value == DBNull.Value ? 0L : Convert.ToInt64(value);
    }

    private static async Task<IReadOnlyList<string>> GetColumnsAsync(
        SqlConnection connection,
        string fullTableName,
        CancellationToken cancellationToken)
    {
        const string sql = """
SELECT c.name
FROM sys.columns AS c
WHERE c.object_id = OBJECT_ID(@tableName)
ORDER BY c.column_id;
""";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@tableName", fullTableName);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        if (columns.Count == 0)
        {
            throw new InvalidOperationException($"Table {fullTableName} was not found.");
        }

        return columns;
    }

    private static List<string> GetCommonColumns(IReadOnlyList<string> sourceColumns, IReadOnlyList<string> destinationColumns)
    {
        var destinationSet = new HashSet<string>(destinationColumns, StringComparer.OrdinalIgnoreCase);
        return sourceColumns.Where(destinationSet.Contains).ToList();
    }

    private static List<string> SelectFirstAvailableKeySet(IReadOnlyList<string> columns, params string[][] keySets)
    {
        var set = new HashSet<string>(columns, StringComparer.OrdinalIgnoreCase);
        foreach (var keySet in keySets)
        {
            if (keySet.All(set.Contains))
            {
                return keySet.ToList();
            }
        }

        return new List<string>();
    }

    private static void EnsureColumnsExist(string tableName, IReadOnlyList<string> availableColumns, IReadOnlyList<string> requiredColumns)
    {
        var set = new HashSet<string>(availableColumns, StringComparer.OrdinalIgnoreCase);
        var missing = requiredColumns.Where(c => !set.Contains(c)).ToList();
        if (missing.Count > 0)
        {
            throw new InvalidOperationException($"Table {tableName} is missing required columns: {string.Join(", ", missing)}");
        }
    }

    private static async Task<DataTable> ReadDataTableAsync(SqlCommand command, CancellationToken cancellationToken)
    {
        var table = new DataTable();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        table.Load(reader);
        return table;
    }

    private static async Task ExecuteNonQueryAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string BuildColumnList(IEnumerable<string> columns)
        => string.Join(", ", columns.Select(QuoteIdentifier));

    private static string QuoteIdentifier(string identifier)
        => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";

    private static void ValidateOptions(SyncOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.SourceConnectionString))
        {
            throw new InvalidOperationException("Sync:SourceConnectionString is required.");
        }

        if (string.IsNullOrWhiteSpace(options.DestinationConnectionString))
        {
            throw new InvalidOperationException("Sync:DestinationConnectionString is required.");
        }

        if (string.IsNullOrWhiteSpace(options.SyncName))
        {
            throw new InvalidOperationException("Sync:SyncName is required.");
        }

        if (options.MaxSection2RowsToTransfer is <= 0)
        {
            throw new InvalidOperationException("Sync:MaxSection2RowsToTransfer must be greater than zero when specified.");
        }
    }
}
