using StreamApi.Extensions;
using StreamApi.Logging;
using StreamApi.Performance;
using StreamApi.PostgreSQL;
using StreamApi.Routing;

var builder = WebApplication.CreateBuilder(args);

builder.AddStreamApiLogging();

builder.Configuration.AddJsonFile(
    "Configuration/tables.json",
    optional: false,
    reloadOnChange: true);

builder.Configuration.AddJsonFile(
    "monitoring-toko_api.json",
    optional: false,
    reloadOnChange: true);

builder.Services.AddStreamApi(
    builder.Configuration);

var app = builder.Build();


// ------------------------------------------------------------
// Resource baseline
// ------------------------------------------------------------

var resourceMonitor =
    app.Services.GetRequiredService<ResourceMonitor>();

var workerBudget =
    app.Services.GetRequiredService<WorkerBudget>();

var baseline =
    await resourceMonitor.CaptureBaselineAsync(
        CancellationToken.None);

workerBudget.Initialize(
    baseline);

var logger =
    app.Services
        .GetRequiredService<ILoggerFactory>()
        .CreateLogger("Startup");

logger.LogInformation(
    "[RESOURCE_BASELINE] processors={Processors} systemCpu={SystemCpu:F1}% processCpu={ProcessCpu:F1}% totalMemoryMb={TotalMemoryMb:F0} availableMemoryMb={AvailableMemoryMb:F0} availableMemory={AvailableMemoryPercent:F1}% processMemoryMb={ProcessMemoryMb:F0} initialLifecycleConcurrency={InitialLifecycleConcurrency} maximumLifecycleConcurrency={MaximumLifecycleConcurrency}",
    baseline.ProcessorCount,
    baseline.SystemCpuPercent,
    baseline.ProcessCpuPercent,
    baseline.TotalMemoryBytes / 1024d / 1024d,
    baseline.AvailableMemoryBytes / 1024d / 1024d,
    baseline.AvailableMemoryPercent,
    baseline.ProcessMemoryBytes / 1024d / 1024d,
    workerBudget.LifecycleConcurrency,
    workerBudget.MaximumLifecycleConcurrency);


// ------------------------------------------------------------
// Durable database initialization
// ------------------------------------------------------------

var checkpointInitializer =
    app.Services
        .GetRequiredService<PostgreSqlDatabaseInitializer>();

var memoryCheckpointStore =
    app.Services
        .GetRequiredService<ReconciliationCheckpointStore>();

var connectionFactory =
    app.Services
        .GetRequiredService<PostgreSqlConnectionFactory>();

await using (
    var catalogConnection =
        connectionFactory.CreateCatalogConnection())
{
    await catalogConnection.OpenAsync();

    // --------------------------------------------------------
    // Processing checkpoint
    // --------------------------------------------------------

    await checkpointInitializer
        .EnsureProcessingCheckpointTableAsync(
            catalogConnection,
            CancellationToken.None);

    await checkpointInitializer
        .EnsureProcessingCheckpointStateTableAsync(
            catalogConnection,
            CancellationToken.None);



    // --------------------------------------------------------
    // Durable processing-event state
    //
    // Stores the selected batch size for each event.
    //
    // This makes batch boundaries survive application restart.
    // --------------------------------------------------------

    await checkpointInitializer
        .EnsureProcessingEventStateTableAsync(
            catalogConnection,
            CancellationToken.None);
}


// ------------------------------------------------------------
// Load durable checkpoint state into memory
// ------------------------------------------------------------

await memoryCheckpointStore.LoadAsync(
    CancellationToken.None);

logger.LogInformation(
    "[CHECKPOINT_INITIALIZATION_COMPLETE]");


// ------------------------------------------------------------
// Start StreamApi pipeline
// ------------------------------------------------------------

app.UseStreamApi();

app.Run();