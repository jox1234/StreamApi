using System.Diagnostics;
using System.Text.Json;
using StreamApi.Processing.Monitoring;
using StreamApi.Processing.StreamedData;
using StreamApi.Routing;

namespace StreamApi.Api.MonitoringToko;

public sealed class MonitoringTokoService
{
    private readonly RoutingStore _routingStore;

    private readonly TokoReader _tokoReader;

    private readonly StreamedDataStore _streamedDataStore;

    private readonly FileMonitoringStore _fileMonitoringStore;

    private readonly ILogger<MonitoringTokoService> _logger;

    public MonitoringTokoService(
        RoutingStore routingStore,
        TokoReader tokoReader,
        StreamedDataStore streamedDataStore,
        FileMonitoringStore fileMonitoringStore,
        ILogger<MonitoringTokoService> logger)
    {
        _routingStore = routingStore;

        _tokoReader = tokoReader;

        _streamedDataStore = streamedDataStore;

        _fileMonitoringStore = fileMonitoringStore;

        _logger = logger;
    }

    public async Task<
        IReadOnlyList<
            Dictionary<string, JsonElement>>>
        GetAsync(
            string tableName,
            string storeCode,
            string branchCode,
            long tglProses,
            CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var stopwatch =
            Stopwatch.StartNew();

        _logger.LogInformation(
            "[MONITORING_TOKO_REQUEST] " +
            "table={TableName} " +
            "toko={KodeToko} " +
            "branchCode={BranchCode} " +
            "tglProses={TglProses}",
            tableName,
            storeCode,
            branchCode,
            tglProses);

        /*
         * ------------------------------------------------------------
         * 1. Validate toko exists in routing
         * ------------------------------------------------------------
         */

        if (!_routingStore.Current.TryGetRoute(
                storeCode,
                out var route)
            || route is null)
        {
            _logger.LogWarning(
                "[MONITORING_TOKO_STORE_NOT_FOUND] " +
                "table={TableName} " +
                "toko={KodeToko} " +
                "branchCode={BranchCode} " +
                "tglProses={TglProses}",
                tableName,
                storeCode,
                branchCode,
                tglProses);

            throw new MonitoringTokoStoreNotFoundException(
                storeCode);
        }

        /*
         * ------------------------------------------------------------
         * 2. Validate actual branch code
         * ------------------------------------------------------------
         */

        var actualBranchCode =
            await _tokoReader.GetBranchCodeAsync(
                storeCode,
                cancellationToken);

        if (actualBranchCode is null)
        {
            _logger.LogWarning(
                "[MONITORING_TOKO_BRANCH_NOT_FOUND] " +
                "table={TableName} " +
                "toko={KodeToko} " +
                "branchCode={BranchCode} " +
                "tglProses={TglProses}",
                tableName,
                storeCode,
                branchCode,
                tglProses);

            throw new MonitoringTokoBranchNotFoundException(
                storeCode);
        }

        if (!string.Equals(
                actualBranchCode,
                branchCode,
                StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "[MONITORING_TOKO_BRANCH_MISMATCH] " +
                "table={TableName} " +
                "toko={KodeToko} " +
                "requestedBranchCode={RequestedBranchCode} " +
                "actualBranchCode={ActualBranchCode} " +
                "tglProses={TglProses}",
                tableName,
                storeCode,
                branchCode,
                actualBranchCode,
                tglProses);

            throw new MonitoringTokoBranchMismatchException(
                storeCode,
                branchCode,
                actualBranchCode);
        }

        /*
         * ------------------------------------------------------------
         * 3. Try RAM snapshot
         *
         * IMPORTANT:
         *
         * The presence of a RAM snapshot does NOT mean that the
         * snapshot is complete.
         *
         * StreamedDataStore receives rows batch-by-batch while the
         * source is still being processed.
         *
         * Therefore RAM may temporarily contain:
         *
         *     batch 1
         *     batch 2
         *     ...
         *
         * while more batches are still being streamed.
         *
         * completed.json is the durable completion barrier.
         *
         * Only expose RAM when completed.json exists.
         * ------------------------------------------------------------
         */

        if (_streamedDataStore.TryGet(
                tableName,
                storeCode,
                tglProses,
                out var snapshot)
            && snapshot is not null)
        {
            var snapshotCompleted =
                _fileMonitoringStore.IsSnapshotCompleted(
                    tableName,
                    storeCode,
                    tglProses);

            if (snapshotCompleted)
            {
                var rows =
                    snapshot.GetAllRows();

                _logger.LogInformation(
                    "[MONITORING_TOKO_RAM_HIT] " +
                    "table={TableName} " +
                    "toko={KodeToko} " +
                    "branchCode={BranchCode} " +
                    "tglProses={TglProses} " +
                    "rows={RowCount} " +
                    "batches={BatchCount}",
                    tableName,
                    storeCode,
                    branchCode,
                    tglProses,
                    rows.Count,
                    snapshot.BatchCount);

                _logger.LogInformation(
                    "[MONITORING_TOKO_RESPONSE] " +
                    "table={TableName} " +
                    "toko={KodeToko} " +
                    "branchCode={BranchCode} " +
                    "tglProses={TglProses} " +
                    "source=RAM " +
                    "rows={RowCount} " +
                    "elapsedMs={ElapsedMs:F2}",
                    tableName,
                    storeCode,
                    branchCode,
                    tglProses,
                    rows.Count,
                    stopwatch.Elapsed.TotalMilliseconds);

                return rows;
            }

            /*
             * RAM exists, but the durable completion marker does not.
             *
             * This means processing is still in progress, or the
             * durable snapshot has not successfully completed.
             *
             * NEVER expose this partial RAM snapshot.
             */

            var partialRows =
                snapshot.GetAllRows();

            _logger.LogInformation(
                "[MONITORING_TOKO_RAM_INCOMPLETE] " +
                "table={TableName} " +
                "toko={KodeToko} " +
                "branchCode={BranchCode} " +
                "tglProses={TglProses} " +
                "rows={RowCount} " +
                "batches={BatchCount} " +
                "completedMarker=false",
                tableName,
                storeCode,
                branchCode,
                tglProses,
                partialRows.Count,
                snapshot.BatchCount);
        }
        else
        {
            _logger.LogInformation(
                "[MONITORING_TOKO_RAM_MISS] " +
                "table={TableName} " +
                "toko={KodeToko} " +
                "branchCode={BranchCode} " +
                "tglProses={TglProses}",
                tableName,
                storeCode,
                branchCode,
                tglProses);
        }

        /*
         * ------------------------------------------------------------
         * 4. Durable file fallback
         *
         * ReadCompletedSnapshotAsync() only returns data when
         * completed.json exists.
         *
         * Therefore this path can never expose a partial snapshot.
         * ------------------------------------------------------------
         */

        _logger.LogInformation(
            "[MONITORING_TOKO_FILE_FALLBACK_STARTED] " +
            "table={TableName} " +
            "toko={KodeToko} " +
            "branchCode={BranchCode} " +
            "tglProses={TglProses}",
            tableName,
            storeCode,
            branchCode,
            tglProses);

        var fileRows =
            await _fileMonitoringStore
                .ReadCompletedSnapshotAsync(
                    tableName,
                    storeCode,
                    tglProses,
                    cancellationToken);

        /*
         * ------------------------------------------------------------
         * 5. No completed snapshot available
         * ------------------------------------------------------------
         */

        if (fileRows.Count == 0)
        {
            _logger.LogInformation(
                "[MONITORING_TOKO_FILE_EMPTY] " +
                "table={TableName} " +
                "toko={KodeToko} " +
                "branchCode={BranchCode} " +
                "tglProses={TglProses} " +
                "rows=0",
                tableName,
                storeCode,
                branchCode,
                tglProses);

            _logger.LogInformation(
                "[MONITORING_TOKO_RESPONSE] " +
                "table={TableName} " +
                "toko={KodeToko} " +
                "branchCode={BranchCode} " +
                "tglProses={TglProses} " +
                "source=FILE " +
                "rows=0 " +
                "elapsedMs={ElapsedMs:F2}",
                tableName,
                storeCode,
                branchCode,
                tglProses,
                stopwatch.Elapsed.TotalMilliseconds);

            return [];
        }

        /*
         * ------------------------------------------------------------
         * 6. Return completed durable snapshot
         * ------------------------------------------------------------
         */

        _logger.LogInformation(
            "[MONITORING_TOKO_FILE_FALLBACK] " +
            "table={TableName} " +
            "toko={KodeToko} " +
            "branchCode={BranchCode} " +
            "tglProses={TglProses} " +
            "rows={RowCount}",
            tableName,
            storeCode,
            branchCode,
            tglProses,
            fileRows.Count);

        _logger.LogInformation(
            "[MONITORING_TOKO_RESPONSE] " +
            "table={TableName} " +
            "toko={KodeToko} " +
            "branchCode={BranchCode} " +
            "tglProses={TglProses} " +
            "source=FILE " +
            "rows={RowCount} " +
            "elapsedMs={ElapsedMs:F2}",
            tableName,
            storeCode,
            branchCode,
            tglProses,
            fileRows.Count,
            stopwatch.Elapsed.TotalMilliseconds);

        return fileRows;
    }
}