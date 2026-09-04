using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace VRDataSyncService;

public sealed class Worker : BackgroundService
{
    private static readonly HashSet<int> TransientSqlErrorNumbers =
    [
        -2,
        53,
        1205,
        233,
        701,
        10928,
        10929,
        40197,
        40501,
        40613
    ];

    private static SyncResilienceOptions _resilience = new();
    private static SyncPerformanceOptions _performance = new();
    private static readonly string[] SectionMeta2PrimaryMergeKeys = ["ccdr_id", "section_start_time"];
    private static readonly string[] SectionMeta2FallbackMergeKeys = ["ccdr_id"];
    private static readonly string[] SectionCdrMedia2PrimaryMergeKeys = ["id"];
    private static readonly string[] SectionCdrMedia2FallbackMergeKeys = ["ccdr_id", "section_start_time"];

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

        _resilience = new SyncResilienceOptions
        {
            CommandTimeoutSeconds = Math.Max(30, _options.Resilience.CommandTimeoutSeconds),
            BulkCopyTimeoutSeconds = Math.Max(0, _options.Resilience.BulkCopyTimeoutSeconds),
            MaxRetryAttempts = Math.Max(1, _options.Resilience.MaxRetryAttempts),
            RetryBaseDelaySeconds = Math.Max(1, _options.Resilience.RetryBaseDelaySeconds),
            RetryMaxDelaySeconds = Math.Max(1, _options.Resilience.RetryMaxDelaySeconds)
        };

        var normalizedMinBatchSize = Math.Max(1, _options.Performance.MinBatchSize);
        var normalizedMaxBatchSize = Math.Max(normalizedMinBatchSize, _options.Performance.MaxBatchSize);

        _performance = new SyncPerformanceOptions
        {
            EnableAdaptiveBatchSizing = _options.Performance.EnableAdaptiveBatchSizing,
            MinBatchSize = normalizedMinBatchSize,
            MaxBatchSize = normalizedMaxBatchSize,
            SlowBatchThresholdSeconds = Math.Max(1, _options.Performance.SlowBatchThresholdSeconds),
            FastBatchThresholdSeconds = Math.Max(1, _options.Performance.FastBatchThresholdSeconds)
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            ValidateOptions(_options);

            var batchSize = Math.Max(1, _options.BatchSize);
            var syncName = _options.SyncName.Trim();
            var testModeEnabled = _options.TestMode.Enabled;

            var section2BatchSize = ResolveBatchSize(_options.BatchSizeOverrides.Section2, batchSize);
            var sectionMeta2BatchSize = ResolveBatchSize(_options.BatchSizeOverrides.SectionMeta2, batchSize);
            var sectionCentera2BatchSize = ResolveBatchSize(_options.BatchSizeOverrides.SectionCentera2, batchSize);
            var sectionCdrMedia2BatchSize = ResolveBatchSize(_options.BatchSizeOverrides.SectionCdrMedia2, batchSize);

            var section2MaxRows = testModeEnabled ? _options.TestMode.MaxRows.Section2 : null;
            var sectionMeta2MaxRows = testModeEnabled ? _options.TestMode.MaxRows.SectionMeta2 : null;
            var sectionCentera2MaxRows = testModeEnabled ? _options.TestMode.MaxRows.SectionCentera2 : null;
            var sectionCdrMedia2MaxRows = testModeEnabled ? _options.TestMode.MaxRows.SectionCdrMedia2 : null;

            var configuredSection2SourceTotalRows = _options.SourceTotalRows.Section2;
            var configuredSectionMeta2SourceTotalRows = _options.SourceTotalRows.SectionMeta2;
            var configuredSectionCentera2SourceTotalRows = _options.SourceTotalRows.SectionCentera2;
            var configuredSectionCdrMedia2SourceTotalRows = _options.SourceTotalRows.SectionCdrMedia2;

            _logger.LogInformation(
                "Sync startup. SyncName={SyncName}, BaseBatchSize={BaseBatchSize}, BatchSizeOverrides(section2/meta2/centera2/cdr_media2)=({Section2BatchSize}/{SectionMeta2BatchSize}/{SectionCentera2BatchSize}/{SectionCdrMedia2BatchSize}), TestModeEnabled={TestModeEnabled}, MaxRows(section2/meta2/centera2/cdr_media2)=({Section2MaxRows}/{SectionMeta2MaxRows}/{SectionCentera2MaxRows}/{SectionCdrMedia2MaxRows}), SourceTotalRows(section2/meta2/centera2/cdr_media2)=({Section2SourceTotalRows}/{SectionMeta2SourceTotalRows}/{SectionCentera2SourceTotalRows}/{SectionCdrMedia2SourceTotalRows}), AdaptiveBatchSizing={EnableAdaptiveBatchSizing}, AdaptiveRange={MinBatchSize}-{MaxBatchSize}, Slow/FastThresholdSec={SlowBatchThresholdSeconds}/{FastBatchThresholdSeconds}, CommandTimeoutSec={CommandTimeoutSeconds}, BulkCopyTimeoutSec={BulkCopyTimeoutSeconds}, MaxRetryAttempts={MaxRetryAttempts}",
                syncName,
                batchSize,
                section2BatchSize,
                sectionMeta2BatchSize,
                sectionCentera2BatchSize,
                sectionCdrMedia2BatchSize,
                testModeEnabled,
                section2MaxRows,
                sectionMeta2MaxRows,
                sectionCentera2MaxRows,
                sectionCdrMedia2MaxRows,
                configuredSection2SourceTotalRows,
                configuredSectionMeta2SourceTotalRows,
                configuredSectionCentera2SourceTotalRows,
                configuredSectionCdrMedia2SourceTotalRows,
                _performance.EnableAdaptiveBatchSizing,
                _performance.MinBatchSize,
                _performance.MaxBatchSize,
                _performance.SlowBatchThresholdSeconds,
                _performance.FastBatchThresholdSeconds,
                _resilience.CommandTimeoutSeconds,
                _resilience.BulkCopyTimeoutSeconds,
                _resilience.MaxRetryAttempts);

            await using var sourceConnection = new SqlConnection(_options.SourceConnectionString);
            await using var destinationConnection = new SqlConnection(_options.DestinationConnectionString);

            await ExecuteWithRetryAsync(
                token => EnsureSourceDatabaseExistsAsync(token),
                "Ensure source database exists",
                stoppingToken);

            await ExecuteWithRetryAsync(
                token => sourceConnection.OpenAsync(token),
                "Open source connection",
                stoppingToken);
            _logger.LogInformation("Source connection opened.");

            await ExecuteWithRetryAsync(
                token => LogSourceIndexDiagnosticsAsync(sourceConnection, token),
                "Check source index coverage",
                stoppingToken);

            await ExecuteWithRetryAsync(
                token => EnsureDestinationDatabaseExistsAsync(token),
                "Ensure destination database exists",
                stoppingToken);

            await ExecuteWithRetryAsync(
                token => destinationConnection.OpenAsync(token),
                "Open destination connection",
                stoppingToken);
            _logger.LogInformation("Destination connection opened.");

            await ExecuteWithRetryAsync(
                token => EnsureProgressTableAsync(destinationConnection, token),
                "Ensure progress table",
                stoppingToken);
            _logger.LogInformation("SyncProgress table is ready.");

            _logger.LogInformation("Starting table sync: dbo.staging_section2");
            await SyncSection2Async(sourceConnection, destinationConnection, syncName, section2BatchSize, section2MaxRows, configuredSection2SourceTotalRows, stoppingToken);
            _logger.LogInformation("Finished table sync: dbo.staging_section2");

            _logger.LogInformation("Starting table sync: dbo.staging_section_meta2");
            await SyncSectionMeta2Async(sourceConnection, destinationConnection, syncName, sectionMeta2BatchSize, sectionMeta2MaxRows, configuredSectionMeta2SourceTotalRows, stoppingToken);
            _logger.LogInformation("Finished table sync: dbo.staging_section_meta2");

            _logger.LogInformation("Starting table sync: dbo.staging_section_centera2");
            await SyncSectionCentera2Async(sourceConnection, destinationConnection, syncName, sectionCentera2BatchSize, sectionCentera2MaxRows, configuredSectionCentera2SourceTotalRows, stoppingToken);
            _logger.LogInformation("Finished table sync: dbo.staging_section_centera2");

            _logger.LogInformation("Starting table sync: dbo.staging_section_cdr_media2");
            await SyncSectionCdrMedia2Async(sourceConnection, destinationConnection, syncName, sectionCdrMedia2BatchSize, sectionCdrMedia2MaxRows, configuredSectionCdrMedia2SourceTotalRows, stoppingToken);
            _logger.LogInformation("Finished table sync: dbo.staging_section_cdr_media2");

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
        long? maxRowsToProcess,
        long? configuredSourceTotalRows,
        CancellationToken cancellationToken)
    {
        const string sourceTableName = "dbo.section2";
        const string destinationTableName = "dbo.staging_section2";
        const string keyColumn = "CCDR_ID";

        var sourceColumns = await GetColumnsAsync(source, sourceTableName, cancellationToken);
        var destinationColumns = await GetColumnsAsync(destination, destinationTableName, cancellationToken);
        var commonColumns = GetCommonColumns(sourceColumns, destinationColumns);

        if (!commonColumns.Any(c => c.Equals(keyColumn, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException($"{sourceTableName} and {destinationTableName} must contain {keyColumn}.");
        }

        await SyncInBatchesAsync(
            destination,
            syncName,
            destinationTableName,
            keyColumn,
            batchSize,
            maxRowsToProcess,
            configuredSourceTotalRows,
            fetchSourceTotalRowsAsync: ct => CountRowsAsync(source, sourceTableName, ct),
            fetchBoundaryAsync: (lastCcdrId, currentBatchSize, ct) => GetParentBoundaryAsync(source, lastCcdrId, currentBatchSize, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadSection2RowsAsync(source, sourceTableName, commonColumns, lastCcdrId, maxCcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => MergeBatchAsync(destination, tx, destinationTableName, commonColumns, [keyColumn], rows, ct),
            cancellationToken: cancellationToken);
    }

    private async Task SyncSectionMeta2Async(
        SqlConnection source,
        SqlConnection destination,
        string syncName,
        int batchSize,
        long? maxRowsToProcess,
        long? configuredSourceTotalRows,
        CancellationToken cancellationToken)
    {
        const string sourceTableName = "dbo.section_meta2";
        const string destinationTableName = "dbo.staging_section_meta2";

        var sourceColumns = await GetColumnsAsync(source, sourceTableName, cancellationToken);
        var destinationColumns = await GetColumnsAsync(destination, destinationTableName, cancellationToken);
        var commonColumns = GetCommonColumns(sourceColumns, destinationColumns);

        var mergeKeys = SelectFirstAvailableKeySet(
            commonColumns,
            SectionMeta2PrimaryMergeKeys,
            SectionMeta2FallbackMergeKeys);

        if (mergeKeys.Count == 0)
        {
            throw new InvalidOperationException($"Unable to determine a merge key for {destinationTableName}.");
        }

        await SyncInBatchesAsync(
            destination,
            syncName,
            destinationTableName,
            "ccdr_id",
            batchSize,
            maxRowsToProcess,
            configuredSourceTotalRows,
            fetchSourceTotalRowsAsync: ct => CountRowsAsync(source, sourceTableName, ct),
            fetchBoundaryAsync: (lastCcdrId, currentBatchSize, ct) => GetParentBoundaryAsync(source, lastCcdrId, currentBatchSize, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadTableRangeAsync(source, sourceTableName, commonColumns, "ccdr_id", lastCcdrId, maxCcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => MergeBatchAsync(destination, tx, destinationTableName, commonColumns, mergeKeys, rows, ct),
            cancellationToken: cancellationToken);
    }

    private async Task SyncSectionCentera2Async(
        SqlConnection source,
        SqlConnection destination,
        string syncName,
        int batchSize,
        long? maxRowsToProcess,
        long? configuredSourceTotalRows,
        CancellationToken cancellationToken)
    {
        const string sourceTableName = "dbo.section_centera2";
        const string destinationTableName = "dbo.staging_section_centera2";

        var destinationColumns = await GetColumnsAsync(destination, destinationTableName, cancellationToken);
        var requiredDestinationColumns = new[]
        {
            "ccdr_id", "extension", "clip_id", "moved_to_section_centera2", "ts_insert1", "section_start_time"
        };

        EnsureColumnsExist(destinationTableName, destinationColumns, requiredDestinationColumns);

        await SyncInBatchesAsync(
            destination,
            syncName,
            destinationTableName,
            "ccdr_id",
            batchSize,
            maxRowsToProcess,
            configuredSourceTotalRows,
            fetchSourceTotalRowsAsync: ct => CountRowsAsync(source, sourceTableName, ct),
            fetchBoundaryAsync: (lastCcdrId, currentBatchSize, ct) => GetParentBoundaryAsync(source, lastCcdrId, currentBatchSize, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadSectionCenteraRowsAsync(source, sourceTableName, lastCcdrId, maxCcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => InsertIfNotExistsAsync(
                destination,
                tx,
                destinationTableName,
                requiredDestinationColumns,
                requiredDestinationColumns,
                rows,
                ct),
            cancellationToken: cancellationToken);
    }

    private async Task SyncSectionCdrMedia2Async(
        SqlConnection source,
        SqlConnection destination,
        string syncName,
        int batchSize,
        long? maxRowsToProcess,
        long? configuredSourceTotalRows,
        CancellationToken cancellationToken)
    {
        const string sourceTableName = "dbo.section_cdr_media2";
        const string destinationTableName = "dbo.staging_section_cdr_media2";

        var sourceColumns = await GetColumnsAsync(source, sourceTableName, cancellationToken);
        var destinationColumns = await GetColumnsAsync(destination, destinationTableName, cancellationToken);
        var commonColumns = GetCommonColumns(sourceColumns, destinationColumns)
            .Where(c => !c.Equals("section_start_time", StringComparison.OrdinalIgnoreCase))
            .ToList();

        commonColumns.Add("section_start_time");

        EnsureColumnsExist(destinationTableName, destinationColumns, commonColumns);

        var mergeKeys = SelectFirstAvailableKeySet(
            commonColumns,
            SectionCdrMedia2PrimaryMergeKeys,
            SectionCdrMedia2FallbackMergeKeys);

        await SyncInBatchesAsync(
            destination,
            syncName,
            destinationTableName,
            "ccdr_id",
            batchSize,
            maxRowsToProcess,
            configuredSourceTotalRows,
            fetchSourceTotalRowsAsync: ct => CountRowsAsync(source, sourceTableName, ct),
            fetchBoundaryAsync: (lastCcdrId, currentBatchSize, ct) => GetParentBoundaryAsync(source, lastCcdrId, currentBatchSize, ct),
            fetchRowsAsync: (lastCcdrId, maxCcdrId, ct) => ReadSectionCdrMediaRowsAsync(source, sourceTableName, commonColumns, lastCcdrId, maxCcdrId, ct),
            writeBatchAsync: (rows, tx, ct) => mergeKeys.Count > 0
                ? MergeBatchAsync(destination, tx, destinationTableName, commonColumns, mergeKeys, rows, ct)
                : InsertIfNotExistsAsync(destination, tx, destinationTableName, commonColumns, commonColumns, rows, ct),
            cancellationToken: cancellationToken);
    }

    private async Task SyncInBatchesAsync(
        SqlConnection destination,
        string syncName,
        string destinationTableName,
        string checkpointColumn,
        int batchSize,
        long? maxRowsToProcess,
        long? configuredSourceTotalRows,
        Func<CancellationToken, Task<long>> fetchSourceTotalRowsAsync,
        Func<string?, int, CancellationToken, Task<(bool hasRows, string? maxCcdrId)>> fetchBoundaryAsync,
        Func<string?, string?, CancellationToken, Task<DataTable>> fetchRowsAsync,
        Func<DataTable, SqlTransaction, CancellationToken, Task> writeBatchAsync,
        CancellationToken cancellationToken)
    {
        var lastCcdrId = await ExecuteWithRetryAsync(
            token => GetLastCcdrIdAsync(destination, syncName, destinationTableName, token),
            $"Read last checkpoint for {destinationTableName}",
            cancellationToken);

        var existingRowsProcessed = await ExecuteWithRetryAsync(
            token => GetRowsProcessedAsync(destination, syncName, destinationTableName, token),
            $"Read rows processed for {destinationTableName}",
            cancellationToken);
        var remainingRowsToProcess = maxRowsToProcess.HasValue
            ? Math.Max(0, maxRowsToProcess.Value - existingRowsProcessed)
            : (long?)null;

        var effectiveBatchSize = _performance.EnableAdaptiveBatchSizing
            ? Math.Clamp(batchSize, _performance.MinBatchSize, _performance.MaxBatchSize)
            : batchSize;

        var runStopwatch = Stopwatch.StartNew();
        long rowsProcessedThisRun = 0;
        var batchNumber = 0;

        try
        {
            var sourceTotalRows = await ResolveSourceTotalRowsAsync(
                destinationTableName,
                configuredSourceTotalRows,
                maxRowsToProcess,
                fetchSourceTotalRowsAsync,
                cancellationToken);

            if (sourceTotalRows < existingRowsProcessed)
            {
                _logger.LogWarning(
                    "Configured/derived source total rows {SourceTotalRows} is less than existing rows_processed {ExistingRowsProcessed} for {TableName}. Progress totals may reflect prior runs.",
                    sourceTotalRows,
                    existingRowsProcessed,
                    destinationTableName);
            }

            _logger.LogInformation(
                "Starting sync for {TableName}. Existing processed rows: {ExistingRowsProcessed}. Source target rows: {SourceTotalRows}. Last checkpoint CCDR_ID: {LastCcdrId}",
                destinationTableName,
                existingRowsProcessed,
                sourceTotalRows,
                lastCcdrId ?? "<none>");

            await UpsertProgressAsync(
                destination,
                transaction: null,
                syncName,
                destinationTableName,
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

                if (remainingRowsToProcess is 0)
                {
                    _logger.LogInformation(
                        "Reached configured max rows for {TableName}. No more batches required.",
                        destinationTableName);
                    break;
                }

                var currentBatchSize = remainingRowsToProcess.HasValue
                    ? (int)Math.Min(effectiveBatchSize, remainingRowsToProcess.Value)
                    : effectiveBatchSize;

                var isNearCompletionBatch = remainingRowsToProcess.HasValue && remainingRowsToProcess.Value <= currentBatchSize;
                batchNumber++;

                if (remainingRowsToProcess.HasValue)
                {
                    _logger.LogInformation(
                        "Preparing batch #{BatchNumber} for {TableName}. Requested batch size: {CurrentBatchSize}. Remaining requested rows: {RemainingRows}. Current checkpoint: {LastCcdrId}",
                        batchNumber,
                        destinationTableName,
                        currentBatchSize,
                        remainingRowsToProcess.Value,
                        lastCcdrId ?? "<none>");
                }
                else
                {
                    _logger.LogInformation(
                        "Preparing batch #{BatchNumber} for {TableName}. Requested batch size: {CurrentBatchSize}. Current checkpoint: {LastCcdrId}",
                        batchNumber,
                        destinationTableName,
                        currentBatchSize,
                        lastCcdrId ?? "<none>");
                }

                var batchCycleStopwatch = Stopwatch.StartNew();
                var boundaryStopwatch = Stopwatch.StartNew();
                var (hasRows, maxCcdrId) = await ExecuteWithRetryAsync(
                    token => fetchBoundaryAsync(lastCcdrId, currentBatchSize, token),
                    $"Fetch boundary for {destinationTableName}",
                    cancellationToken);
                boundaryStopwatch.Stop();

                if (!hasRows || string.IsNullOrWhiteSpace(maxCcdrId))
                {
                    if (isNearCompletionBatch && remainingRowsToProcess.HasValue && remainingRowsToProcess.Value > 0)
                    {
                        _logger.LogWarning(
                            "Near completion for {TableName}, but no additional boundary rows were found. Remaining requested rows: {RemainingRows}",
                            destinationTableName,
                            remainingRowsToProcess.Value);
                    }
                    else
                    {
                        _logger.LogInformation(
                            "No additional boundary rows found for {TableName}. Sync range has been exhausted at checkpoint {LastCcdrId}.",
                            destinationTableName,
                            lastCcdrId ?? "<none>");
                    }

                    break;
                }

                _logger.LogInformation(
                    "Boundary resolved for batch #{BatchNumber} on {TableName}. Batch max CCDR_ID: {MaxCcdrId}. Boundary lookup elapsed: {BoundaryElapsed}",
                    batchNumber,
                    destinationTableName,
                    maxCcdrId,
                    FormatDuration(boundaryStopwatch.Elapsed));

                if (!string.IsNullOrWhiteSpace(lastCcdrId)
                    && string.Compare(maxCcdrId, lastCcdrId, StringComparison.OrdinalIgnoreCase) <= 0)
                {
                    throw new InvalidOperationException(
                        $"Invalid boundary for {destinationTableName}: max CCDR_ID {maxCcdrId} is not greater than last CCDR_ID {lastCcdrId}.");
                }

                var readStopwatch = Stopwatch.StartNew();
                var fetchedRows = await ExecuteWithRetryAsync(
                    token => fetchRowsAsync(lastCcdrId, maxCcdrId, token),
                    $"Read rows for {destinationTableName}",
                    cancellationToken);
                readStopwatch.Stop();

                DataTable rows = fetchedRows;
                if (remainingRowsToProcess.HasValue && fetchedRows.Rows.Count > remainingRowsToProcess.Value)
                {
                    var trimmedRows = TakeTopRows(fetchedRows, (int)remainingRowsToProcess.Value);
                    if (!ReferenceEquals(trimmedRows, fetchedRows))
                    {
                        rows = trimmedRows;
                        fetchedRows.Dispose();
                    }
                }

                var batchRowCount = rows.Rows.Count;

                _logger.LogInformation(
                    "Read completed for batch #{BatchNumber} on {TableName}. Rows fetched: {RowsFetched}. Read elapsed: {ReadElapsed}",
                    batchNumber,
                    destinationTableName,
                    batchRowCount,
                    FormatDuration(readStopwatch.Elapsed));

                if (batchRowCount == 0)
                {
                    rows.Dispose();
                    throw new InvalidOperationException(
                        $"Boundary lookup returned CCDR_ID {maxCcdrId} for {destinationTableName}, but the data read returned zero rows.");
                }

                try
                {
                    var checkpointCcdrId = GetMaxCcdrIdFromRows(rows, checkpointColumn) ?? maxCcdrId;

                    _logger.LogInformation(
                        "Writing batch #{BatchNumber} for {TableName}. Checkpoint CCDR_ID to persist: {CheckpointCcdrId}",
                        batchNumber,
                        destinationTableName,
                        checkpointCcdrId);

                    var writeStopwatch = Stopwatch.StartNew();
                    await ExecuteWithRetryAsync(
                        async token =>
                        {
                            await using var tx = await destination.BeginTransactionAsync(token);
                            await writeBatchAsync(rows, (SqlTransaction)tx, token);
                            await UpsertProgressAsync(
                                destination,
                                (SqlTransaction)tx,
                                syncName,
                                destinationTableName,
                                checkpointCcdrId,
                                batchRowCount,
                                sourceTotalRows: null,
                                status: "running",
                                completedAtUtc: null,
                                lastError: null,
                                token);
                            await tx.CommitAsync(token);
                        },
                        $"Write batch for {destinationTableName}",
                        cancellationToken);
                    writeStopwatch.Stop();

                    lastCcdrId = checkpointCcdrId;
                    rowsProcessedThisRun += batchRowCount;
                    if (remainingRowsToProcess.HasValue)
                    {
                        remainingRowsToProcess -= batchRowCount;
                    }

                    var totalProcessed = existingRowsProcessed + rowsProcessedThisRun;

                    var validationStopwatch = Stopwatch.StartNew();
                    await ValidateBatchCompletionAsync(
                        destination,
                        syncName,
                        destinationTableName,
                        checkpointCcdrId,
                        totalProcessed,
                        remainingRowsToProcess,
                        cancellationToken);
                    validationStopwatch.Stop();

                    _logger.LogInformation(
                        "Batch #{BatchNumber} validation succeeded for {TableName}. Persisted checkpoint: {CheckpointCcdrId}. Persisted rows processed target: {TotalProcessed}. Validation elapsed: {ValidationElapsed}",
                        batchNumber,
                        destinationTableName,
                        checkpointCcdrId,
                        totalProcessed,
                        FormatDuration(validationStopwatch.Elapsed));

                    batchCycleStopwatch.Stop();

                    var remainingRows = Math.Max(0L, sourceTotalRows - totalProcessed);
                    var elapsedSeconds = Math.Max(runStopwatch.Elapsed.TotalSeconds, 0.001D);
                    var averageRowsPerSecond = rowsProcessedThisRun / elapsedSeconds;
                    var dataPhaseRowsPerSecond = batchRowCount / Math.Max(readStopwatch.Elapsed.TotalSeconds + writeStopwatch.Elapsed.TotalSeconds, 0.001D);
                    var endToEndBatchRowsPerSecond = batchRowCount / Math.Max(batchCycleStopwatch.Elapsed.TotalSeconds, 0.001D);
                    TimeSpan? eta = averageRowsPerSecond > 0
                        ? TimeSpan.FromSeconds(remainingRows / averageRowsPerSecond)
                        : null;

                    var managedMemoryMb = GC.GetTotalMemory(false) / (1024D * 1024D);

                    _logger.LogInformation(
                        "Batch synced for {TableName}. Last CCDR_ID: {LastCcdrId}. Batch rows: {BatchRows}. Total processed: {TotalProcessed}/{SourceTotalRows}. Data-phase rate: {DataPhaseRowsPerSecond:F2} rows/sec. End-to-end batch rate: {EndToEndBatchRowsPerSecond:F2} rows/sec. Avg rate: {AverageRowsPerSecond:F2} rows/sec. Timings [boundary/read/write/validate/total]: {BoundaryElapsed}/{ReadElapsed}/{WriteElapsed}/{ValidationElapsed}/{TotalElapsed}. Managed memory: {ManagedMemoryMb:F2} MB. ETA: {Eta}",
                        destinationTableName,
                        lastCcdrId,
                        batchRowCount,
                        totalProcessed,
                        sourceTotalRows,
                        dataPhaseRowsPerSecond,
                        endToEndBatchRowsPerSecond,
                        averageRowsPerSecond,
                        FormatDuration(boundaryStopwatch.Elapsed),
                        FormatDuration(readStopwatch.Elapsed),
                        FormatDuration(writeStopwatch.Elapsed),
                        FormatDuration(validationStopwatch.Elapsed),
                        FormatDuration(batchCycleStopwatch.Elapsed),
                        managedMemoryMb,
                        eta.HasValue ? FormatDuration(eta.Value) : "N/A");

                    if (_performance.EnableAdaptiveBatchSizing)
                    {
                        var previousBatchSize = effectiveBatchSize;

                        if (batchCycleStopwatch.Elapsed.TotalSeconds >= _performance.SlowBatchThresholdSeconds
                            && effectiveBatchSize > _performance.MinBatchSize)
                        {
                            effectiveBatchSize = Math.Max(_performance.MinBatchSize, effectiveBatchSize / 2);
                        }
                        else if (batchCycleStopwatch.Elapsed.TotalSeconds <= _performance.FastBatchThresholdSeconds
                                 && effectiveBatchSize < _performance.MaxBatchSize)
                        {
                            effectiveBatchSize = Math.Min(
                                _performance.MaxBatchSize,
                                effectiveBatchSize + Math.Max(1, effectiveBatchSize / 4));
                        }

                        if (effectiveBatchSize != previousBatchSize)
                        {
                            _logger.LogInformation(
                                "Adaptive batch size adjusted for {TableName}: {PreviousBatchSize} -> {NewBatchSize} (end-to-end batch elapsed {BatchElapsed}).",
                                destinationTableName,
                                previousBatchSize,
                                effectiveBatchSize,
                                FormatDuration(batchCycleStopwatch.Elapsed));
                        }
                    }
                }
                finally
                {
                    rows.Dispose();
                }
            }

            runStopwatch.Stop();
            var finalRowsPerSecond = rowsProcessedThisRun / Math.Max(runStopwatch.Elapsed.TotalSeconds, 0.001D);

            _logger.LogInformation(
                "Completed sync for {TableName}. Total batches: {BatchCount}. Rows processed this run: {RowsProcessedThisRun}. Elapsed: {Elapsed}. Average rate: {AverageRowsPerSecond:F2} rows/sec. Final checkpoint: {LastCcdrId}",
                destinationTableName,
                batchNumber,
                rowsProcessedThisRun,
                FormatDuration(runStopwatch.Elapsed),
                finalRowsPerSecond,
                lastCcdrId ?? "<none>");

            await UpsertProgressAsync(
                destination,
                transaction: null,
                syncName,
                destinationTableName,
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
                destinationTableName,
                lastCcdrId,
                rowsIncrement: 0,
                sourceTotalRows: null,
                status: "failed",
                completedAtUtc: null,
                lastError: ex.ToString(),
                cancellationToken);

            _logger.LogError(ex, "Failed syncing {TableName}.", destinationTableName);
            throw;
        }
    }

    private async Task EnsureSourceDatabaseExistsAsync(CancellationToken cancellationToken)
    {
        var sourceBuilder = new SqlConnectionStringBuilder(_options.SourceConnectionString);
        var sourceDatabaseName = sourceBuilder.InitialCatalog;

        if (string.IsNullOrWhiteSpace(sourceDatabaseName))
        {
            throw new InvalidOperationException("Source connection string must include a database name.");
        }

        var sourceServerConnectionBuilder = new SqlConnectionStringBuilder(_options.SourceConnectionString)
        {
            InitialCatalog = "master"
        };

        await using var sourceServerConnection = new SqlConnection(sourceServerConnectionBuilder.ConnectionString);
        await sourceServerConnection.OpenAsync(cancellationToken);

        const string sql = "SELECT DB_ID(@databaseName);";
        await using var command = CreateCommand(sql, sourceServerConnection);
        command.Parameters.AddWithValue("@databaseName", sourceDatabaseName);

        var databaseId = await command.ExecuteScalarAsync(cancellationToken);
        if (databaseId is null || databaseId == DBNull.Value)
        {
            throw new InvalidOperationException($"Source database {sourceDatabaseName} does not exist or is not accessible.");
        }

        _logger.LogInformation("Source database {DatabaseName} is available.", sourceDatabaseName);
    }

    private async Task EnsureDestinationDatabaseExistsAsync(CancellationToken cancellationToken)
    {
        var destinationBuilder = new SqlConnectionStringBuilder(_options.DestinationConnectionString);
        var destinationDatabaseName = destinationBuilder.InitialCatalog;

        if (string.IsNullOrWhiteSpace(destinationDatabaseName))
        {
            throw new InvalidOperationException("Destination connection string must include a database name.");
        }

        var serverConnectionBuilder = new SqlConnectionStringBuilder(_options.DestinationConnectionString)
        {
            InitialCatalog = "master"
        };

        await using var serverConnection = new SqlConnection(serverConnectionBuilder.ConnectionString);
        await serverConnection.OpenAsync(cancellationToken);

        const string sql = """
DECLARE @created bit = 0;
IF DB_ID(@databaseName) IS NULL
BEGIN
    DECLARE @createSql nvarchar(max) = N'CREATE DATABASE ' + QUOTENAME(@databaseName) + N';';
    EXEC (@createSql);
    SET @created = 1;
END

SELECT @created;
""";

        await using var command = CreateCommand(sql, serverConnection);
        command.Parameters.AddWithValue("@databaseName", destinationDatabaseName);

        var created = await command.ExecuteScalarAsync(cancellationToken);
        if (created is not null && created != DBNull.Value && Convert.ToBoolean(created))
        {
            _logger.LogInformation("Created destination database {DatabaseName}.", destinationDatabaseName);
        }
        else
        {
            _logger.LogInformation("Destination database {DatabaseName} already exists.", destinationDatabaseName);
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

        await using var command = CreateCommand(commandText, destination);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<(bool hasRows, string? maxCcdrId)> GetParentBoundaryAsync(
        SqlConnection source,
        string? lastCcdrId,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var sql = lastCcdrId is null
            ? """
SELECT TOP (1) batch.CCDR_ID
FROM
(
    SELECT TOP (@batchSize) s.CCDR_ID
    FROM dbo.section2 AS s
    ORDER BY s.CCDR_ID
) AS batch
ORDER BY batch.CCDR_ID DESC;
"""
            : """
SELECT TOP (1) batch.CCDR_ID
FROM
(
    SELECT TOP (@batchSize) s.CCDR_ID
    FROM dbo.section2 AS s
    WHERE s.CCDR_ID > @lastCcdrId
    ORDER BY s.CCDR_ID
) AS batch
ORDER BY batch.CCDR_ID DESC;
""";

        await using var command = CreateCommand(sql, source);
        command.Parameters.AddWithValue("@batchSize", batchSize);
        if (lastCcdrId is not null)
        {
            command.Parameters.AddWithValue("@lastCcdrId", lastCcdrId);
        }

        var value = await command.ExecuteScalarAsync(cancellationToken);
        var maxCcdrId = value is null || value == DBNull.Value ? null : Convert.ToString(value);
        return (!string.IsNullOrWhiteSpace(maxCcdrId), maxCcdrId);
    }

    private static async Task<DataTable> ReadSection2RowsAsync(
        SqlConnection source,
        string sourceTableName,
        IReadOnlyList<string> columns,
        string? lastCcdrId,
        string? maxCcdrId,
        CancellationToken cancellationToken)
    {
        var selectColumns = BuildColumnList(columns);
        var sql = lastCcdrId is null
            ? $"""
SELECT {selectColumns}
FROM {sourceTableName} AS s
WHERE s.CCDR_ID <= @maxCcdrId
ORDER BY s.CCDR_ID;
"""
            : $"""
SELECT {selectColumns}
FROM {sourceTableName} AS s
WHERE s.CCDR_ID > @lastCcdrId
  AND s.CCDR_ID <= @maxCcdrId
ORDER BY s.CCDR_ID;
""";

        await using var command = CreateCommand(sql, source);
        if (lastCcdrId is not null)
        {
            command.Parameters.AddWithValue("@lastCcdrId", lastCcdrId);
        }

        command.Parameters.AddWithValue("@maxCcdrId", maxCcdrId ?? throw new InvalidOperationException("maxCcdrId is required."));

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
        var sql = lastCcdrId is null
            ? $"""
SELECT {selectColumns}
FROM {tableName} AS t
WHERE t.{QuoteIdentifier(ccdrColumn)} <= @maxCcdrId
ORDER BY t.{QuoteIdentifier(ccdrColumn)};
"""
            : $"""
SELECT {selectColumns}
FROM {tableName} AS t
WHERE t.{QuoteIdentifier(ccdrColumn)} > @lastCcdrId
  AND t.{QuoteIdentifier(ccdrColumn)} <= @maxCcdrId
ORDER BY t.{QuoteIdentifier(ccdrColumn)};
""";

        await using var command = CreateCommand(sql, source);
        if (lastCcdrId is not null)
        {
            command.Parameters.AddWithValue("@lastCcdrId", lastCcdrId);
        }

        command.Parameters.AddWithValue("@maxCcdrId", maxCcdrId ?? throw new InvalidOperationException("maxCcdrId is required."));

        return await ReadDataTableAsync(command, cancellationToken);
    }

    private static async Task<DataTable> ReadSectionCenteraRowsAsync(
        SqlConnection source,
        string sourceTableName,
        string? lastCcdrId,
        string? maxCcdrId,
        CancellationToken cancellationToken)
    {
        var sql = lastCcdrId is null
            ? $"""
SELECT
    c.ccdr_id,
    c.extension,
    c.clip_id,
    c.moved_to_section_centera2,
    c.ts_insert1,
    s.Start_Time AS section_start_time
FROM {sourceTableName} AS c
LEFT JOIN dbo.section2 AS s
    ON s.CCDR_ID = c.ccdr_id
WHERE c.ccdr_id <= @maxCcdrId
ORDER BY c.ccdr_id;
"""
            : $"""
SELECT
    c.ccdr_id,
    c.extension,
    c.clip_id,
    c.moved_to_section_centera2,
    c.ts_insert1,
    s.Start_Time AS section_start_time
FROM {sourceTableName} AS c
LEFT JOIN dbo.section2 AS s
    ON s.CCDR_ID = c.ccdr_id
WHERE c.ccdr_id > @lastCcdrId
  AND c.ccdr_id <= @maxCcdrId
ORDER BY c.ccdr_id;
""";

        await using var command = CreateCommand(sql, source);
        if (lastCcdrId is not null)
        {
            command.Parameters.AddWithValue("@lastCcdrId", lastCcdrId);
        }

        command.Parameters.AddWithValue("@maxCcdrId", maxCcdrId ?? throw new InvalidOperationException("maxCcdrId is required."));

        return await ReadDataTableAsync(command, cancellationToken);
    }

    private static async Task<DataTable> ReadSectionCdrMediaRowsAsync(
        SqlConnection source,
        string sourceTableName,
        IReadOnlyList<string> columns,
        string? lastCcdrId,
        string? maxCcdrId,
        CancellationToken cancellationToken)
    {
        var projection = columns.Select(c =>
            c.Equals("section_start_time", StringComparison.OrdinalIgnoreCase)
                ? "s.Start_Time AS section_start_time"
                : $"m.{QuoteIdentifier(c)}");

        var sql = lastCcdrId is null
            ? $"""
SELECT {string.Join(", ", projection)}
FROM {sourceTableName} AS m
LEFT JOIN dbo.section2 AS s
    ON s.CCDR_ID = m.ccdr_id
WHERE m.ccdr_id <= @maxCcdrId
ORDER BY m.ccdr_id;
"""
            : $"""
SELECT {string.Join(", ", projection)}
FROM {sourceTableName} AS m
LEFT JOIN dbo.section2 AS s
    ON s.CCDR_ID = m.ccdr_id
WHERE m.ccdr_id > @lastCcdrId
  AND m.ccdr_id <= @maxCcdrId
ORDER BY m.ccdr_id;
""";

        await using var command = CreateCommand(sql, source);
        if (lastCcdrId is not null)
        {
            command.Parameters.AddWithValue("@lastCcdrId", lastCcdrId);
        }

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
            BulkCopyTimeout = _resilience.BulkCopyTimeoutSeconds,
            EnableStreaming = true
        };

        foreach (var column in columns)
        {
            bulkCopy.ColumnMappings.Add(column, column);
        }

        using var reader = rows.CreateDataReader();
        await bulkCopy.WriteToServerAsync(reader, cancellationToken);
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

        await using var command = CreateCommand(sql, destination, transaction);
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

    private async Task<long> ResolveSourceTotalRowsAsync(
        string tableName,
        long? configuredSourceTotalRows,
        long? maxRowsToProcess,
        Func<CancellationToken, Task<long>> fetchSourceTotalRowsAsync,
        CancellationToken cancellationToken)
    {
        if (configuredSourceTotalRows.HasValue)
        {
            var configuredTotal = configuredSourceTotalRows.Value;
            if (maxRowsToProcess.HasValue)
            {
                configuredTotal = Math.Min(configuredTotal, maxRowsToProcess.Value);
            }

            _logger.LogInformation(
                "Using configured source total rows for {TableName}: {SourceTotalRows}",
                tableName,
                configuredTotal);

            return configuredTotal;
        }

        if (maxRowsToProcess.HasValue)
        {
            _logger.LogInformation(
                "No configured source total rows for {TableName}. Using test-mode max rows as source target: {SourceTotalRows}",
                tableName,
                maxRowsToProcess.Value);

            return maxRowsToProcess.Value;
        }

        _logger.LogWarning(
            "No configured source total rows for {TableName}. Falling back to source COUNT_BIG query; this may be slow.",
            tableName);

        return await ExecuteWithRetryAsync(
            token => fetchSourceTotalRowsAsync(token),
            $"Count source rows for {tableName}",
            cancellationToken);
    }

    private async Task ValidateBatchCompletionAsync(
        SqlConnection destination,
        string syncName,
        string tableName,
        string? expectedLastCcdrId,
        long expectedRowsProcessed,
        long? remainingRowsToProcess,
        CancellationToken cancellationToken)
    {
        var (persistedLastCcdrId, persistedRowsProcessed) = await ExecuteWithRetryAsync(
            token => GetProgressSnapshotAsync(destination, syncName, tableName, token),
            $"Validate batch completion for {tableName}",
            cancellationToken);

        if (!string.Equals(persistedLastCcdrId, expectedLastCcdrId, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Progress validation failed for {tableName}. Expected last CCDR_ID {expectedLastCcdrId}, but found {persistedLastCcdrId}." );
        }

        if (persistedRowsProcessed < expectedRowsProcessed)
        {
            throw new InvalidOperationException(
                $"Progress validation failed for {tableName}. Expected rows_processed >= {expectedRowsProcessed}, but found {persistedRowsProcessed}." );
        }

        if (remainingRowsToProcess is not null && remainingRowsToProcess.Value == 0)
        {
            _logger.LogInformation(
                "Final requested boundary reached for {TableName}. Last CCDR_ID: {LastCcdrId}. Total processed: {RowsProcessed}",
                tableName,
                persistedLastCcdrId,
                persistedRowsProcessed);
        }
    }

    private static async Task<(string? lastCcdrId, long rowsProcessed)> GetProgressSnapshotAsync(
        SqlConnection destination,
        string syncName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = """
SELECT last_ccdr_id, rows_processed
FROM dbo.SyncProgress
WHERE sync_name = @syncName
  AND table_name = @tableName;
""";

        await using var command = CreateCommand(sql, destination);
        command.Parameters.AddWithValue("@syncName", syncName);
        command.Parameters.AddWithValue("@tableName", tableName);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, 0L);
        }

        var lastCcdrId = reader.IsDBNull(0) ? null : reader.GetString(0);
        var rowsProcessed = reader.IsDBNull(1) ? 0L : reader.GetInt64(1);
        return (lastCcdrId, rowsProcessed);
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

        await using var command = CreateCommand(sql, destination);
        command.Parameters.AddWithValue("@syncName", syncName);
        command.Parameters.AddWithValue("@tableName", tableName);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value == DBNull.Value || value is null ? null : Convert.ToString(value);
    }

    private static async Task<long> GetRowsProcessedAsync(
        SqlConnection destination,
        string syncName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = """
SELECT rows_processed
FROM dbo.SyncProgress
WHERE sync_name = @syncName
  AND table_name = @tableName;
""";

        await using var command = CreateCommand(sql, destination);
        command.Parameters.AddWithValue("@syncName", syncName);
        command.Parameters.AddWithValue("@tableName", tableName);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null || value == DBNull.Value ? 0L : Convert.ToInt64(value);
    }

    private static async Task<long> CountRowsAsync(
        SqlConnection source,
        string tableName,
        CancellationToken cancellationToken)
    {
        await using var command = CreateCommand($"SELECT COUNT_BIG(1) FROM {tableName};", source);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null || value == DBNull.Value ? 0L : Convert.ToInt64(value);
    }

    private static DataTable TakeTopRows(DataTable sourceTable, int rowCount)
    {
        if (rowCount >= sourceTable.Rows.Count)
        {
            return sourceTable;
        }

        var trimmed = sourceTable.Clone();
        for (var i = 0; i < rowCount; i++)
        {
            trimmed.ImportRow(sourceTable.Rows[i]);
        }

        return trimmed;
    }

    private static string? GetMaxCcdrIdFromRows(DataTable rows, string checkpointColumn)
    {
        if (rows.Rows.Count == 0)
        {
            return null;
        }

        var matchingColumn = rows.Columns.Cast<DataColumn>()
            .FirstOrDefault(c => c.ColumnName.Equals(checkpointColumn, StringComparison.OrdinalIgnoreCase));

        if (matchingColumn is null)
        {
            return null;
        }

        var value = rows.Rows[^1][matchingColumn];
        return value == DBNull.Value ? null : Convert.ToString(value);
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

        await using var command = CreateCommand(sql, connection);
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
        return [.. sourceColumns.Where(destinationSet.Contains)];
    }

    private static List<string> SelectFirstAvailableKeySet(IReadOnlyList<string> columns, params string[][] keySets)
    {
        var set = new HashSet<string>(columns, StringComparer.OrdinalIgnoreCase);
        foreach (var keySet in keySets)
        {
            if (keySet.All(set.Contains))
            {
                return [.. keySet];
            }
        }

        return [];
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
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, cancellationToken);
        table.Load(reader);
        return table;
    }

    private static async Task ExecuteNonQueryAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = CreateCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static SqlCommand CreateCommand(string sql, SqlConnection connection, SqlTransaction? transaction = null)
    {
        var command = transaction is null
            ? new SqlCommand(sql, connection)
            : new SqlCommand(sql, connection, transaction);

        command.CommandTimeout = _resilience.CommandTimeoutSeconds;
        return command;
    }

    private async Task ExecuteWithRetryAsync(
        Func<CancellationToken, Task> operation,
        string operationName,
        CancellationToken cancellationToken)
    {
        await ExecuteWithRetryAsync<object?>(
            async token =>
            {
                await operation(token);
                return null;
            },
            operationName,
            cancellationToken);
    }

    private async Task<T> ExecuteWithRetryAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        string operationName,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation(cancellationToken);
            }
            catch (Exception ex) when (IsRetryable(ex) && attempt < _resilience.MaxRetryAttempts)
            {
                var delaySeconds = Math.Min(
                    _resilience.RetryMaxDelaySeconds,
                    _resilience.RetryBaseDelaySeconds * (int)Math.Pow(2, attempt - 1));

                _logger.LogWarning(
                    ex,
                    "Transient failure during {OperationName}. Retry {Attempt}/{MaxAttempts} in {DelaySeconds}s.",
                    operationName,
                    attempt,
                    _resilience.MaxRetryAttempts,
                    delaySeconds);

                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
            }
        }
    }

    private static bool IsRetryable(Exception exception)
    {
        if (exception is TimeoutException)
        {
            return true;
        }

        if (exception is SqlException sqlException)
        {
            return sqlException.Errors.Cast<SqlError>().Any(error => TransientSqlErrorNumbers.Contains(error.Number));
        }

        if (exception is InvalidOperationException invalidOperationException)
        {
            return invalidOperationException.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase)
                || invalidOperationException.Message.Contains("transient", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static int ResolveBatchSize(int? overrideBatchSize, int defaultBatchSize)
    {
        var resolved = overrideBatchSize ?? defaultBatchSize;
        return Math.Max(1, resolved);
    }

    private async Task LogSourceIndexDiagnosticsAsync(SqlConnection source, CancellationToken cancellationToken)
    {
        var checks = new (string tableName, string columnName)[]
        {
            ("dbo.section2", "CCDR_ID"),
            ("dbo.section_meta2", "ccdr_id"),
            ("dbo.section_centera2", "ccdr_id"),
            ("dbo.section_cdr_media2", "ccdr_id")
        };

        foreach (var (tableName, columnName) in checks)
        {
            var hasLeadingIndex = await HasLeadingIndexOnColumnAsync(source, tableName, columnName, cancellationToken);
            if (hasLeadingIndex)
            {
                _logger.LogInformation("Source index check passed for {TableName}.{ColumnName} (leading key index found).", tableName, columnName);
            }
            else
            {
                _logger.LogWarning("Source index check warning for {TableName}.{ColumnName}: no leading-key index found; expect slower keyset scans.", tableName, columnName);
            }
        }
    }

    private static async Task<bool> HasLeadingIndexOnColumnAsync(
        SqlConnection source,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        const string sql = """
SELECT CASE WHEN EXISTS
(
    SELECT 1
    FROM sys.indexes AS i
    INNER JOIN sys.index_columns AS ic
        ON ic.object_id = i.object_id
       AND ic.index_id = i.index_id
    INNER JOIN sys.columns AS c
        ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(@tableName)
      AND i.is_hypothetical = 0
      AND i.type IN (1, 2)
      AND ic.key_ordinal = 1
      AND c.name = @columnName
) THEN 1 ELSE 0 END;
""";

        await using var command = CreateCommand(sql, source);
        command.Parameters.AddWithValue("@tableName", tableName);
        command.Parameters.AddWithValue("@columnName", columnName);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is not null && value != DBNull.Value && Convert.ToInt32(value) == 1;
    }

    private static string BuildColumnList(IEnumerable<string> columns)
        => string.Join(", ", columns.Select(QuoteIdentifier));

    private static string QuoteIdentifier(string identifier)
        => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";

    private static string FormatDuration(TimeSpan duration)
        => $"{(int)duration.TotalDays}d {duration.Hours:D2}h {duration.Minutes:D2}m";

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

        if (options.BatchSize <= 0)
        {
            throw new InvalidOperationException("Sync:BatchSize must be greater than zero.");
        }

        ValidatePositiveBatchSize(options.BatchSizeOverrides.Section2, "Sync:BatchSizeOverrides:Section2");
        ValidatePositiveBatchSize(options.BatchSizeOverrides.SectionMeta2, "Sync:BatchSizeOverrides:SectionMeta2");
        ValidatePositiveBatchSize(options.BatchSizeOverrides.SectionCentera2, "Sync:BatchSizeOverrides:SectionCentera2");
        ValidatePositiveBatchSize(options.BatchSizeOverrides.SectionCdrMedia2, "Sync:BatchSizeOverrides:SectionCdrMedia2");

        if (options.Performance.MinBatchSize <= 0)
        {
            throw new InvalidOperationException("Sync:Performance:MinBatchSize must be greater than zero.");
        }

        if (options.Performance.MaxBatchSize <= 0)
        {
            throw new InvalidOperationException("Sync:Performance:MaxBatchSize must be greater than zero.");
        }

        if (options.Performance.MaxBatchSize < options.Performance.MinBatchSize)
        {
            throw new InvalidOperationException("Sync:Performance:MaxBatchSize must be greater than or equal to MinBatchSize.");
        }

        if (options.Performance.SlowBatchThresholdSeconds <= 0)
        {
            throw new InvalidOperationException("Sync:Performance:SlowBatchThresholdSeconds must be greater than zero.");
        }

        if (options.Performance.FastBatchThresholdSeconds <= 0)
        {
            throw new InvalidOperationException("Sync:Performance:FastBatchThresholdSeconds must be greater than zero.");
        }

        if (options.Performance.FastBatchThresholdSeconds >= options.Performance.SlowBatchThresholdSeconds)
        {
            throw new InvalidOperationException("Sync:Performance:FastBatchThresholdSeconds must be less than SlowBatchThresholdSeconds.");
        }

        if (options.Resilience.CommandTimeoutSeconds <= 0)
        {
            throw new InvalidOperationException("Sync:Resilience:CommandTimeoutSeconds must be greater than zero.");
        }

        if (options.Resilience.BulkCopyTimeoutSeconds < 0)
        {
            throw new InvalidOperationException("Sync:Resilience:BulkCopyTimeoutSeconds must be zero or greater.");
        }

        if (options.Resilience.MaxRetryAttempts <= 0)
        {
            throw new InvalidOperationException("Sync:Resilience:MaxRetryAttempts must be greater than zero.");
        }

        if (options.Resilience.RetryBaseDelaySeconds <= 0)
        {
            throw new InvalidOperationException("Sync:Resilience:RetryBaseDelaySeconds must be greater than zero.");
        }

        if (options.Resilience.RetryMaxDelaySeconds <= 0)
        {
            throw new InvalidOperationException("Sync:Resilience:RetryMaxDelaySeconds must be greater than zero.");
        }

        ValidatePositiveTestCap(options.SourceTotalRows.Section2, "Sync:SourceTotalRows:Section2");
        ValidatePositiveTestCap(options.SourceTotalRows.SectionMeta2, "Sync:SourceTotalRows:SectionMeta2");
        ValidatePositiveTestCap(options.SourceTotalRows.SectionCentera2, "Sync:SourceTotalRows:SectionCentera2");
        ValidatePositiveTestCap(options.SourceTotalRows.SectionCdrMedia2, "Sync:SourceTotalRows:SectionCdrMedia2");

        if (!options.TestMode.Enabled)
        {
            return;
        }

        ValidatePositiveTestCap(options.TestMode.MaxRows.Section2, "Sync:TestMode:MaxRows:Section2");
        ValidatePositiveTestCap(options.TestMode.MaxRows.SectionMeta2, "Sync:TestMode:MaxRows:SectionMeta2");
        ValidatePositiveTestCap(options.TestMode.MaxRows.SectionCentera2, "Sync:TestMode:MaxRows:SectionCentera2");
        ValidatePositiveTestCap(options.TestMode.MaxRows.SectionCdrMedia2, "Sync:TestMode:MaxRows:SectionCdrMedia2");
    }

    private static void ValidatePositiveBatchSize(int? value, string configPath)
    {
        if (value is <= 0)
        {
            throw new InvalidOperationException($"{configPath} must be greater than zero when specified.");
        }
    }

    private static void ValidatePositiveTestCap(long? value, string configPath)
    {
        if (value is <= 0)
        {
            throw new InvalidOperationException($"{configPath} must be greater than zero when specified.");
        }
    }
}
