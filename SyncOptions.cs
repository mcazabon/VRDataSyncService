namespace VRDataSyncService;

public sealed class SyncOptions
{
    public const string SectionName = "Sync";

    public string SyncName { get; set; } = "VFC_AMR_RP_to_VF_AMR_RP1";

    public int BatchSize { get; set; } = 10000;

    public TableSyncBatchSizeOverridesOptions BatchSizeOverrides { get; set; } = new();

    public TableSyncTestModeOptions TestMode { get; set; } = new();

    public TableSyncSourceTotalRowsOptions SourceTotalRows { get; set; } = new();

    public SyncPerformanceOptions Performance { get; set; } = new();

    public SyncResilienceOptions Resilience { get; set; } = new();

    public string SourceConnectionString { get; set; } = string.Empty;

    public string DestinationConnectionString { get; set; } = string.Empty;
}

public sealed class TableSyncBatchSizeOverridesOptions
{
    public int? Section2 { get; set; }

    public int? SectionMeta2 { get; set; }

    public int? SectionCentera2 { get; set; }

    public int? SectionCdrMedia2 { get; set; }
}

public sealed class TableSyncSourceTotalRowsOptions
{
    public long? Section2 { get; set; }

    public long? SectionMeta2 { get; set; }

    public long? SectionCentera2 { get; set; }

    public long? SectionCdrMedia2 { get; set; }
}

public sealed class SyncPerformanceOptions
{
    public bool EnableAdaptiveBatchSizing { get; set; } = true;

    public int MinBatchSize { get; set; } = 1000;

    public int MaxBatchSize { get; set; } = 25000;

    public int SlowBatchThresholdSeconds { get; set; } = 45;

    public int FastBatchThresholdSeconds { get; set; } = 8;
}

public sealed class SyncResilienceOptions
{
    public int CommandTimeoutSeconds { get; set; } = 600;

    public int BulkCopyTimeoutSeconds { get; set; } = 0;

    public int MaxRetryAttempts { get; set; } = 5;

    public int RetryBaseDelaySeconds { get; set; } = 2;

    public int RetryMaxDelaySeconds { get; set; } = 30;
}

public sealed class TableSyncTestModeOptions
{
    public bool Enabled { get; set; }

    public TableSyncMaxRowsOptions MaxRows { get; set; } = new();
}

public sealed class TableSyncMaxRowsOptions
{
    public long? Section2 { get; set; }

    public long? SectionMeta2 { get; set; }

    public long? SectionCentera2 { get; set; }

    public long? SectionCdrMedia2 { get; set; }
}
