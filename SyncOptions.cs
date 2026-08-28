namespace VRDataSyncService;

public sealed class SyncOptions
{
    public const string SectionName = "Sync";

    public string SyncName { get; set; } = "VFC_AMR_RP_to_VF_AMR_RP1";

    public int BatchSize { get; set; } = 10000;

    public long? MaxSection2RowsToTransfer { get; set; }

    public string SourceConnectionString { get; set; } = string.Empty;

    public string DestinationConnectionString { get; set; } = string.Empty;
}
