using Serilog;
using VRDataSyncService;

var builder = Host.CreateApplicationBuilder(args);

var logDirectory = Path.Combine(AppContext.BaseDirectory, "logs");
Directory.CreateDirectory(logDirectory);
var runTimestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
var logFilePath = Path.Combine(logDirectory, $"vrdatasyncservice-{runTimestamp}.log");
builder.Configuration["Serilog:WriteTo:1:Args:path"] = logFilePath;

builder.Logging.ClearProviders();
builder.Services.AddSerilog((services, loggerConfiguration) => loggerConfiguration
    .ReadFrom.Configuration(builder.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext());

builder.Services.Configure<SyncOptions>(builder.Configuration.GetSection(SyncOptions.SectionName));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
