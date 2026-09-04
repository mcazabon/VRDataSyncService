using System.Data;
using System.Globalization;
using System.Net;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .Build();

var options = configuration.GetSection(StatusPublisherOptions.SectionName).Get<StatusPublisherOptions>()
              ?? throw new InvalidOperationException("StatusPublisher configuration section is required.");

ValidateOptions(options);

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.ClearProviders();
    builder.AddSimpleConsole(console =>
    {
        console.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff zzz ";
        console.SingleLine = true;
    });
    builder.SetMinimumLevel(LogLevel.Information);
});

var logger = loggerFactory.CreateLogger("VRDataSyncStatusPublisher");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

logger.LogInformation(
    "Starting status publisher. Output={OutputPath}, PollingIntervalSeconds={PollingIntervalSeconds}, SyncName={SyncName}",
    options.OutputHtmlPath,
    options.PollingIntervalSeconds,
    options.SyncName);

while (!cts.Token.IsCancellationRequested)
{
    try
    {
        var snapshot = await ReadSnapshotAsync(options, cts.Token);
        var html = BuildHtml(snapshot, options);
        await WriteHtmlAtomicallyAsync(options.OutputHtmlPath, html, cts.Token);

        logger.LogInformation(
            "Status page updated. CountsRows={CountsRows}, ProgressRows={ProgressRows}, GeneratedAtUtc={GeneratedAtUtc:O}",
            snapshot.TableCounts.Count,
            snapshot.ProgressRows.Count,
            snapshot.GeneratedAtUtc);
    }
    catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
    {
        break;
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Failed to refresh status page.");
        var errorHtml = BuildErrorHtml(ex);
        await WriteHtmlAtomicallyAsync(options.OutputHtmlPath, errorHtml, CancellationToken.None);
    }

    try
    {
        await Task.Delay(TimeSpan.FromSeconds(options.PollingIntervalSeconds), cts.Token);
    }
    catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
    {
        break;
    }
}

logger.LogInformation("Status publisher stopped.");

static async Task<SyncStatusSnapshot> ReadSnapshotAsync(StatusPublisherOptions options, CancellationToken cancellationToken)
{
    await using var connection = new SqlConnection(options.DestinationConnectionString);
    await connection.OpenAsync(cancellationToken);

    var tableCounts = await ReadTableCountsAsync(connection, options.CommandTimeoutSeconds, cancellationToken);
    var progressRows = await ReadProgressRowsAsync(connection, options.SyncName, options.CommandTimeoutSeconds, cancellationToken);

    return new SyncStatusSnapshot(DateTime.UtcNow, tableCounts, progressRows);
}

static async Task<List<TableCountRow>> ReadTableCountsAsync(SqlConnection connection, int timeoutSeconds, CancellationToken cancellationToken)
{
    const string sql = """
SELECT 'staging_section2' AS table_name, COUNT_BIG(*) AS rows
FROM dbo.staging_section2 WITH (NOLOCK)
UNION ALL
SELECT 'staging_section_meta2', COUNT_BIG(*)
FROM dbo.staging_section_meta2 WITH (NOLOCK)
UNION ALL
SELECT 'staging_section_centera2', COUNT_BIG(*)
FROM dbo.staging_section_centera2 WITH (NOLOCK)
UNION ALL
SELECT 'staging_section_cdr_media2', COUNT_BIG(*)
FROM dbo.staging_section_cdr_media2 WITH (NOLOCK);
""";

    await using var command = new SqlCommand(sql, connection)
    {
        CommandTimeout = timeoutSeconds
    };

    var rows = new List<TableCountRow>();
    await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
    while (await reader.ReadAsync(cancellationToken))
    {
        rows.Add(new TableCountRow(
            TableName: reader.GetString(0),
            Rows: reader.GetInt64(1)));
    }

    return rows;
}

static async Task<List<ProgressRow>> ReadProgressRowsAsync(
    SqlConnection connection,
    string syncName,
    int timeoutSeconds,
    CancellationToken cancellationToken)
{
    const string sql = """
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
        DATEDIFF(SECOND, started_at, ISNULL(completed_at, SYSUTCDATETIME())) AS elapsed_seconds
    FROM dbo.SyncProgress
    WHERE sync_name = @syncName
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
            WHEN rows_processed = 0 THEN NULL
            WHEN source_total_rows IS NULL OR source_total_rows <= rows_processed THEN NULL
            ELSE CAST((elapsed_seconds * 1.0 / rows_processed) * (source_total_rows - rows_processed) AS bigint)
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
    elapsed_seconds,
    eta_seconds,
    last_error
FROM EtaData
ORDER BY
    CASE table_name
        WHEN 'dbo.staging_section2' THEN 1
        WHEN 'dbo.staging_section_meta2' THEN 2
        WHEN 'dbo.staging_section_centera2' THEN 3
        WHEN 'dbo.staging_section_cdr_media2' THEN 4
        ELSE 99
    END;
""";

    await using var command = new SqlCommand(sql, connection)
    {
        CommandTimeout = timeoutSeconds
    };
    command.Parameters.AddWithValue("@syncName", syncName);

    var rows = new List<ProgressRow>();
    await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
    while (await reader.ReadAsync(cancellationToken))
    {
        rows.Add(new ProgressRow(
            SyncName: reader.GetString(0),
            TableName: reader.GetString(1),
            Status: reader.GetString(2),
            LastCcdrId: reader.IsDBNull(3) ? null : reader.GetString(3),
            RowsProcessed: reader.GetInt64(4),
            SourceTotalRows: reader.IsDBNull(5) ? null : reader.GetInt64(5),
            PercentComplete: reader.IsDBNull(6) ? null : reader.GetDecimal(6),
            StartedAt: reader.GetDateTime(7),
            UpdatedAt: reader.GetDateTime(8),
            CompletedAt: reader.IsDBNull(9) ? null : reader.GetDateTime(9),
            ElapsedSeconds: reader.GetInt32(10),
            EtaSeconds: reader.IsDBNull(11) ? null : reader.GetInt64(11),
            LastError: reader.IsDBNull(12) ? null : reader.GetString(12)));
    }

    return rows;
}

static async Task WriteHtmlAtomicallyAsync(string outputPath, string html, CancellationToken cancellationToken)
{
    var fullPath = Path.GetFullPath(outputPath);
    var directory = Path.GetDirectoryName(fullPath)
                    ?? throw new InvalidOperationException($"Cannot resolve directory for output path '{outputPath}'.");

    Directory.CreateDirectory(directory);

    var tempPath = Path.Combine(directory, $"{Path.GetFileName(fullPath)}.{Guid.NewGuid():N}.tmp");

    await File.WriteAllTextAsync(tempPath, html, Encoding.UTF8, cancellationToken);

    if (File.Exists(fullPath))
    {
        File.Delete(fullPath);
    }

    File.Move(tempPath, fullPath);
}

static string BuildHtml(SyncStatusSnapshot snapshot, StatusPublisherOptions options)
{
    var sb = new StringBuilder(16 * 1024);
    sb.AppendLine("<!doctype html>");
    sb.AppendLine("<html lang=\"en\"><head><meta charset=\"utf-8\"/>");
    sb.AppendLine("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>");
    sb.AppendLine($"<meta http-equiv=\"refresh\" content=\"{options.HtmlAutoRefreshSeconds}\" />");
    sb.AppendLine("<title>VR Data Sync Status</title>");
    sb.AppendLine("<style>body{font-family:Segoe UI,Arial,sans-serif;margin:24px;background:#111827;color:#e5e7eb}h1,h2{margin:0 0 12px 0}table{border-collapse:collapse;width:100%;margin-bottom:24px;background:#1f2937}th,td{border:1px solid #374151;padding:8px;text-align:left;vertical-align:top}th{background:#111827}.ok{color:#86efac}.warn{color:#fbbf24}.err{color:#fca5a5}.mono{font-family:Consolas,monospace;word-break:break-all}.meta{margin:0 0 16px 0;color:#9ca3af}</style>");
    sb.AppendLine("</head><body>");
    sb.AppendLine("<h1>VR Data Sync Status</h1>");
    sb.AppendLine($"<p class=\"meta\">Sync Name: <strong>{Html(options.SyncName)}</strong> &nbsp;|&nbsp; Generated (UTC): {snapshot.GeneratedAtUtc:yyyy-MM-dd HH:mm:ss} &nbsp;|&nbsp; Refresh: {options.HtmlAutoRefreshSeconds}s</p>");

    sb.AppendLine("<h2>Destination Staging Row Counts</h2>");
    sb.AppendLine("<table><thead><tr><th>Table</th><th>Rows</th></tr></thead><tbody>");
    foreach (var row in snapshot.TableCounts)
    {
        sb.AppendLine($"<tr><td>{Html(row.TableName)}</td><td>{row.Rows.ToString("N0", CultureInfo.InvariantCulture)}</td></tr>");
    }
    sb.AppendLine("</tbody></table>");

    sb.AppendLine("<h2>Sync Progress</h2>");
    sb.AppendLine("<table><thead><tr><th>Table</th><th>Status</th><th>Rows Processed</th><th>Source Total</th><th>% Complete</th><th>Elapsed</th><th>ETA</th><th>Last CCDR_ID</th><th>Updated (UTC)</th><th>Last Error</th></tr></thead><tbody>");

    foreach (var row in snapshot.ProgressRows)
    {
        var statusClass = row.Status.Equals("completed", StringComparison.OrdinalIgnoreCase)
            ? "ok"
            : row.Status.Equals("failed", StringComparison.OrdinalIgnoreCase)
                ? "err"
                : "warn";

        sb.AppendLine($"<tr><td>{Html(row.TableName)}</td><td class=\"{statusClass}\">{Html(row.Status)}</td><td>{row.RowsProcessed.ToString("N0", CultureInfo.InvariantCulture)}</td><td>{(row.SourceTotalRows.HasValue ? row.SourceTotalRows.Value.ToString("N0", CultureInfo.InvariantCulture) : "-")}</td><td>{(row.PercentComplete.HasValue ? row.PercentComplete.Value.ToString("F2", CultureInfo.InvariantCulture) + "%" : "-")}</td><td>{FormatDuration(row.ElapsedSeconds)}</td><td>{FormatDuration(row.EtaSeconds)}</td><td class=\"mono\">{Html(row.LastCcdrId ?? "-")}</td><td>{row.UpdatedAt:yyyy-MM-dd HH:mm:ss}</td><td class=\"mono\">{Html(row.LastError ?? "-")}</td></tr>");
    }

    sb.AppendLine("</tbody></table>");
    sb.AppendLine("</body></html>");

    return sb.ToString();
}

static string BuildErrorHtml(Exception exception)
{
    var message = Html(exception.ToString());
    var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    return $"<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"/><title>VR Data Sync Status - Error</title></head><body style=\"font-family:Segoe UI,Arial,sans-serif;background:#111827;color:#fca5a5;padding:24px;\"><h1>Status Publisher Error</h1><p>Generated (UTC): {timestamp}</p><pre style=\"white-space:pre-wrap;background:#1f2937;border:1px solid #374151;padding:12px;\">{message}</pre></body></html>";
}

static string FormatDuration(long? totalSeconds)
{
    if (!totalSeconds.HasValue)
    {
        return "-";
    }

    var seconds = Math.Max(0, totalSeconds.Value);
    var duration = TimeSpan.FromSeconds(seconds);
    return $"{(int)duration.TotalDays}d {duration.Hours:D2}h {duration.Minutes:D2}m {duration.Seconds:D2}s";
}

static string Html(string value) => WebUtility.HtmlEncode(value);

static void ValidateOptions(StatusPublisherOptions options)
{
    if (string.IsNullOrWhiteSpace(options.DestinationConnectionString))
    {
        throw new InvalidOperationException("StatusPublisher:DestinationConnectionString is required.");
    }

    if (string.IsNullOrWhiteSpace(options.SyncName))
    {
        throw new InvalidOperationException("StatusPublisher:SyncName is required.");
    }

    if (string.IsNullOrWhiteSpace(options.OutputHtmlPath))
    {
        throw new InvalidOperationException("StatusPublisher:OutputHtmlPath is required.");
    }

    if (options.PollingIntervalSeconds <= 0)
    {
        throw new InvalidOperationException("StatusPublisher:PollingIntervalSeconds must be greater than zero.");
    }

    if (options.HtmlAutoRefreshSeconds <= 0)
    {
        throw new InvalidOperationException("StatusPublisher:HtmlAutoRefreshSeconds must be greater than zero.");
    }

    if (options.CommandTimeoutSeconds <= 0)
    {
        throw new InvalidOperationException("StatusPublisher:CommandTimeoutSeconds must be greater than zero.");
    }
}

internal sealed class StatusPublisherOptions
{
    public const string SectionName = "StatusPublisher";

    public string DestinationConnectionString { get; set; } = string.Empty;

    public string SyncName { get; set; } = string.Empty;

    public string OutputHtmlPath { get; set; } = @"C:\inetpub\wwwroot\vrdatasync\index.html";

    public int PollingIntervalSeconds { get; set; } = 30;

    public int HtmlAutoRefreshSeconds { get; set; } = 30;

    public int CommandTimeoutSeconds { get; set; } = 120;
}

internal sealed record TableCountRow(string TableName, long Rows);

internal sealed record ProgressRow(
    string SyncName,
    string TableName,
    string Status,
    string? LastCcdrId,
    long RowsProcessed,
    long? SourceTotalRows,
    decimal? PercentComplete,
    DateTime StartedAt,
    DateTime UpdatedAt,
    DateTime? CompletedAt,
    int ElapsedSeconds,
    long? EtaSeconds,
    string? LastError);

internal sealed record SyncStatusSnapshot(
    DateTime GeneratedAtUtc,
    IReadOnlyList<TableCountRow> TableCounts,
    IReadOnlyList<ProgressRow> ProgressRows);
