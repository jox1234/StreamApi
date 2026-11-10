using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StreamApi.Api.MonitoringToko;
using StreamApi.Api.Services;
using StreamApi.Api.TargetApi;
using StreamApi.Buffer;
using StreamApi.Configuration;
using StreamApi.Performance;
using StreamApi.PostgreSQL;
using StreamApi.Processing;
using StreamApi.Processing.Batching;
using StreamApi.Processing.Database;
using StreamApi.Processing.Inquiry;
using StreamApi.Processing.Monitoring;
using StreamApi.Processing.SourceReading;
using StreamApi.Processing.StreamedData;
using StreamApi.Processing.TableMapping;
using StreamApi.Processing.TableResolution;
using StreamApi.Routing;
using StreamApi.Runtime;

namespace StreamApi.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddStreamApi(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        // ----------------------------------------------------
        // ASP.NET Core
        // ----------------------------------------------------

        services.AddControllers();

        services.AddEndpointsApiExplorer();

        services.AddSwaggerGen();


        // ----------------------------------------------------
        // Monitoring Toko
        // ----------------------------------------------------

        services.Configure<MonitoringTokoOptions>(
            configuration.GetSection(
                "MonitoringToko"));

        services.AddSingleton<
            MonitoringTokoBasicAuthentication>();

        services.AddSingleton<
            MonitoringTokoService>();


        // ----------------------------------------------------
        // Monitoring File Backup
        // ----------------------------------------------------

        services.Configure<MonitoringBackupOptions>(
            configuration.GetSection(
                "MonitoringBackup"));

        services.AddSingleton<
            FileMonitoringStore>();

        services.AddHostedService<
            FileMonitoringStartupInitializer>();


        // ----------------------------------------------------
        // Inquiry
        // ----------------------------------------------------

        services.AddSingleton<
            InquiryStore>();

        services.AddSingleton<
            InquiryFileBackupStore>();

        services.AddSingleton<
            InquiryService>();

        services.AddHostedService<
            InquiryStartupInitializer>();

        services.AddSingleton<
            InquiryWorker>();

        services.AddHostedService(
            provider =>
                provider.GetRequiredService<
                    InquiryWorker>());


        // ----------------------------------------------------
        // Performance
        // ----------------------------------------------------

        services.AddSingleton<
            BatchSizeController>();

        services.AddSingleton<
            ApplicationRuntimeState>();

        services.AddSingleton<
            IHealthService,
            HealthService>();

        services.AddSingleton<
            ResourceMonitor>();

        services.AddSingleton<
            WorkerBudget>();

        services.AddSingleton<
            TokoReader>();

        services.AddHostedService<
            AdaptivePerformanceWorker>();


        // ----------------------------------------------------
        // Routing
        // ----------------------------------------------------

        services.AddSingleton<
            RoutingStore>();

        services.AddSingleton<
            PartitionMonitorManager>();

        services.AddSingleton<
            PartitionConnectionBudget>();

        services.AddHostedService(
            provider =>
                provider.GetRequiredService<
                    PartitionMonitorManager>());

        services.AddSingleton<
            TokoSchemaReader>();

        services.AddSingleton<
            RoutingValidator>();


        // ----------------------------------------------------
        // PostgreSQL infrastructure
        // ----------------------------------------------------

        services.AddSingleton<
            PostgreSqlDatabaseDiscovery>();

        services.AddSingleton<
            PostgreSqlConnectionFactory>();

        services.AddSingleton<
            PostgreSqlDatabaseInitializer>();

        services.AddSingleton<
            PostgreSqlLogDt3Reader>();


        // ----------------------------------------------------
        // Reconciliation / checkpoint
        // ----------------------------------------------------

        services.AddSingleton<
            ReconciliationCheckpointStore>();


        // ----------------------------------------------------
        // Source reading
        // ----------------------------------------------------

        services.AddSingleton<
            ISourceReader,
            SourceReader>();

        services.AddSingleton<
            ISourceQueryPlanner,
            SourceQueryPlanner>();

        services.AddSingleton<
            SourceTableMetadataCache>();

        services.AddHostedService<
            ReconciliationScheduler>();


        // ----------------------------------------------------
        // Processing queue / runtime
        // ----------------------------------------------------

        services.AddSingleton<
            ProcessingEventQueue>();

        services.AddSingleton<
            ProcessingRuntimeMetrics>();

        services.AddSingleton<
            ProcessingEventTracker>();

        services.AddSingleton<
            StreamedDataStore>();


        // ----------------------------------------------------
        // Processing event pipeline
        // ----------------------------------------------------

        services.AddSingleton<
            IProcessingEventHandler,
            ProcessingEventHandler>();

        services.AddHostedService<
            ProcessingEventWorker>();


        // ----------------------------------------------------
        // Processing connection
        // ----------------------------------------------------

        services.AddSingleton<
            IProcessingConnectionResolver,
            ProcessingConnectionResolver>();

        services.AddSingleton<
            IProcessingConnectionProvider,
            ProcessingConnectionProvider>();


        // ----------------------------------------------------
        // Table mapping / resolution
        // ----------------------------------------------------

        services.AddSingleton<
            ITableResolver,
            TableResolver>();

        services.AddSingleton<
            TableMappingResolver>();


        // ----------------------------------------------------
        // Durable processing state
        // ----------------------------------------------------

        services.AddSingleton<
            PostgreSqlCheckpointStore>();

        services.AddSingleton<
            PostgreSqlProcessingEventStateStore>();

        services.AddSingleton<
            ProcessingRetryPolicy>();


        // ----------------------------------------------------
        // Routing refresh
        // ----------------------------------------------------

        services.AddHostedService<
            RoutingRefreshWorker>();


        // ----------------------------------------------------
        // Table configuration
        // ----------------------------------------------------

        var tablesConfiguration =
            new TablesConfiguration();

        configuration
            .GetSection("tables")
            .Bind(
                tablesConfiguration.Tables);

        services.AddSingleton(
            new TableRegistry(
                tablesConfiguration.Tables));


        // ----------------------------------------------------
        // Buffer
        // ----------------------------------------------------

        services.AddHostedService<
            BufferWorker>();


        // ----------------------------------------------------
        // Target API
        //
        // POST only for now.
        //
        // JWT / login will be added later when /login
        // becomes available.
        // ----------------------------------------------------

        services.Configure<TargetApiOptions>(
        configuration.GetSection(
            "TargetApi"));

            services.AddSingleton<
                TargetApiPayloadBuilder>();

            services.AddHttpClient<
                ITargetApiClient,
                TargetApiClient>(
                client =>
                {
                    var baseUrl =
                        configuration[
                            "TargetApi:BaseUrl"];

                    if (string.IsNullOrWhiteSpace(
                            baseUrl))
                    {
                        throw new InvalidOperationException(
                            "TargetApi:BaseUrl is not configured.");
                    }

                    client.BaseAddress =
                        new Uri(baseUrl);

                    client.Timeout =
                        TimeSpan.FromSeconds(30);
                });


        return services;
    }
}