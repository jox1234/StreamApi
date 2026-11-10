using Npgsql;
using StreamApi.PostgreSQL;
using StreamApi.Routing;

namespace StreamApi.Processing.Inquiry;

public sealed class InquiryService
{
    private readonly InquiryStore _store;
    private readonly InquiryFileBackupStore _fileBackupStore;
    private readonly IConfiguration _configuration;
    private readonly PostgreSqlConnectionFactory _connectionFactory;
    private readonly PostgreSqlLogDt3Reader _reader;
    private readonly PartitionConnectionBudget _connectionBudget;
    private readonly ILogger<InquiryService> _logger;

    private readonly SemaphoreSlim _fileInitializationLock =
        new(1, 1);

    private readonly SemaphoreSlim _persistenceLock =
        new(1, 1);

    private readonly TaskCompletionSource<bool>
        _initializationCompletion =
            new(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);

    private bool _fileInitialized;

    public InquiryService(
        InquiryStore store,
        InquiryFileBackupStore fileBackupStore,
        IConfiguration configuration,
        PostgreSqlConnectionFactory connectionFactory,
        PostgreSqlLogDt3Reader reader,
        PartitionConnectionBudget connectionBudget,
        ILogger<InquiryService> logger)
    {
        _store = store;
        _fileBackupStore = fileBackupStore;
        _configuration = configuration;
        _connectionFactory =
            connectionFactory;
        _reader =
            reader;
        _connectionBudget =
            connectionBudget;
        _logger = logger;
    }

    public bool IsInitialized =>
        _initializationCompletion
            .Task
            .IsCompletedSuccessfully;

    public int RowCount =>
        _store.Count;

    public IReadOnlyList<LogDt3Row> GetSnapshot()
    {
        return _store.GetSnapshot();
    }

    public async Task WaitUntilInitializedAsync(
        CancellationToken cancellationToken)
    {
        await _initializationCompletion
            .Task
            .WaitAsync(
                cancellationToken);
    }

    /*
     * ------------------------------------------------------------
     * File initialization
     * ------------------------------------------------------------
     *
     * This restores the durable daily Inquiry JSON files.
     *
     * It does NOT mark Inquiry as fully initialized.
     *
     * Database bootstrap must still run afterward.
     */
    public async Task InitializeAsync(
        CancellationToken cancellationToken)
    {
        await _fileInitializationLock.WaitAsync(
            cancellationToken);

        try
        {
            if (_fileInitialized)
            {
                return;
            }

            var today =
                GetProcessingDate();

            var retentionDays =
                GetRetentionDays();

            var cutoffDate =
                today.AddDays(
                    -retentionDays);

            _logger.LogInformation(
                "[INQUIRY_FILE_INITIALIZATION_STARTED] " +
                "today={Today} " +
                "retentionDays={RetentionDays} " +
                "cutoffDate={CutoffDate}",
                today,
                retentionDays,
                cutoffDate);

            await _fileBackupStore
                .CleanupExpiredAsync(
                    retentionDays,
                    cancellationToken);

            var allRows =
                new List<LogDt3Row>();

            for (
                var date = cutoffDate;
                date <= today;
                date = date.AddDays(1))
            {
                var rows =
                    await _fileBackupStore
                        .ReadAsync(
                            date,
                            cancellationToken);

                if (rows.Count == 0)
                {
                    continue;
                }

                allRows.AddRange(rows);

                _logger.LogInformation(
                    "[INQUIRY_FILE_INITIALIZATION_FILE_LOADED] " +
                    "date={Date} " +
                    "rows={RowCount}",
                    date,
                    rows.Count);
            }

            _store.Replace(
                allRows,
                cutoffDate);

            _fileInitialized = true;

            _logger.LogInformation(
                "[INQUIRY_FILE_INITIALIZATION_COMPLETED] " +
                "rows={RowCount}",
                _store.Count);
        }
        finally
        {
            _fileInitializationLock.Release();
        }
    }

    /*
     * ------------------------------------------------------------
     * Database bootstrap
     * ------------------------------------------------------------
     *
     * The caller supplies the current partition monitor
     * snapshots.
     *
     * This keeps InquiryService independent from
     * PartitionMonitorManager and prevents a circular DI
     * dependency:
     *
     *     PartitionMonitorManager
     *             ↓
     *        InquiryWorker
     *             ↓
     *        InquiryService
     *
     * InquiryService no longer points back to
     * PartitionMonitorManager.
     */
    public async Task InitializeFromDatabaseAsync(
        IReadOnlyCollection<PartitionMonitorSnapshot> snapshots,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            snapshots);

        if (IsInitialized)
        {
            return;
        }

        var processingDate =
            GetProcessingDate();

        var retentionDays =
            GetRetentionDays();

        var cutoffDate =
            processingDate.AddDays(
                -retentionDays);

        var supportedPartitions =
            snapshots
                .Where(
                    x =>
                        x.State ==
                        PartitionMonitorState.Running)
                .OrderBy(
                    x => x.PartitionId)
                .ToArray();

        if (supportedPartitions.Length == 0)
        {
            throw new InvalidOperationException(
                "No running POSIDM partition monitors are available for Inquiry bootstrap.");
        }

        _logger.LogInformation(
            "[INQUIRY_DATABASE_INITIALIZATION_STARTED] " +
            "partitions={PartitionCount} " +
            "cutoffDate={CutoffDate} " +
            "retentionDays={RetentionDays}",
            supportedPartitions.Length,
            cutoffDate,
            retentionDays);

        var allRows =
            new List<LogDt3Row>();

        var successfulPartitions = 0;

        foreach (var partition in supportedPartitions)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IDisposable? connectionLease = null;
            NpgsqlConnection? connection = null;

            try
            {
                _logger.LogDebug(
                    "[INQUIRY_DATABASE_PARTITION_WAITING] " +
                    "partition={PartitionId} " +
                    "database={Database}",
                    partition.PartitionId,
                    partition.DatabaseName);

                connectionLease =
                    await _connectionBudget
                        .AcquireAsync(
                            cancellationToken);

                connection =
                    _connectionFactory
                        .CreateConnection(
                            partition.DatabaseName);

                await connection.OpenAsync(
                    cancellationToken);

                _logger.LogDebug(
                    "[INQUIRY_DATABASE_PARTITION_CONNECTED] " +
                    "partition={PartitionId} " +
                    "database={Database}",
                    partition.PartitionId,
                    connection.Database);

                var rows =
                    await _reader
                        .ReadRetainedRowsAsync(
                            connection,
                            partition,
                            cutoffDate,
                            cancellationToken);

                allRows.AddRange(rows);

                successfulPartitions++;

                _logger.LogInformation(
                    "[INQUIRY_DATABASE_PARTITION_COMPLETE] " +
                    "partition={PartitionId} " +
                    "database={Database} " +
                    "rows={RowCount}",
                    partition.PartitionId,
                    partition.DatabaseName,
                    rows.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[INQUIRY_DATABASE_PARTITION_FAILED] " +
                    "partition={PartitionId} " +
                    "database={Database}",
                    partition.PartitionId,
                    partition.DatabaseName);

                throw;
            }
            finally
            {
                if (connection is not null)
                {
                    try
                    {
                        await connection.CloseAsync();
                    }
                    catch
                    {
                    }

                    await connection.DisposeAsync();
                }

                connectionLease?.Dispose();
            }
        }

        /*
         * Only replace the RAM snapshot after ALL supported
         * partitions have been read successfully.
         *
         * This prevents a partial database scan from destroying
         * an otherwise valid durable file snapshot.
         */
        if (successfulPartitions !=
            supportedPartitions.Length)
        {
            throw new InvalidOperationException(
                "Inquiry database initialization did not successfully read every supported partition.");
        }

        _store.Replace(
            allRows,
            cutoffDate);

        _logger.LogInformation(
            "[INQUIRY_DATABASE_INITIALIZATION_RAM_REPLACED] " +
            "partitions={PartitionCount} " +
            "rows={RowCount} " +
            "cutoffDate={CutoffDate}",
            successfulPartitions,
            _store.Count,
            cutoffDate);

        /*
         * Persist the authoritative reconstructed snapshot
         * into today's daily file.
         */
        await PersistCurrentSnapshotAsync(
            processingDate,
            cancellationToken);

        _initializationCompletion
            .TrySetResult(true);

        _logger.LogInformation(
            "[INQUIRY_DATABASE_INITIALIZATION_COMPLETED] " +
            "partitions={PartitionCount} " +
            "rows={RowCount} " +
            "processingDate={ProcessingDate}",
            successfulPartitions,
            _store.Count,
            processingDate);
    }

    /*
     * ------------------------------------------------------------
     * Runtime notification processing
     * ------------------------------------------------------------
     */
    public bool ProcessNotification(
        LogDt3Row row)
    {
        ArgumentNullException.ThrowIfNull(
            row);

        var processingDate =
            GetProcessingDate();

        var retentionDays =
            GetRetentionDays();

        var cutoffDate =
            processingDate.AddDays(
                -retentionDays);

        var rowDate =
            GetTglProsesDate(
                row.TglProses);

        if (rowDate < cutoffDate)
        {
            _logger.LogDebug(
                "[INQUIRY_NOTIFICATION_EXPIRED] " +
                "toko={KodeToko} " +
                "jenisFile={JenisFile} " +
                "tglProses={TglProses} " +
                "rowDate={RowDate} " +
                "cutoffDate={CutoffDate}",
                row.KodeToko,
                row.JenisFile,
                row.TglProses,
                rowDate,
                cutoffDate);

            return false;
        }

        _store.AddOrUpdate(
            row,
            cutoffDate);

        _logger.LogDebug(
            "[INQUIRY_NOTIFICATION_PROCESSED] " +
            "toko={KodeToko} " +
            "jenisFile={JenisFile} " +
            "tglProses={TglProses} " +
            "rows={RowCount}",
            row.KodeToko,
            row.JenisFile,
            row.TglProses,
            _store.Count);

        return true;
    }

    /*
     * ------------------------------------------------------------
     * Persistence
     * ------------------------------------------------------------
     */
    public async Task PersistCurrentSnapshotAsync(
        CancellationToken cancellationToken)
    {
        var processingDate =
            GetProcessingDate();

        await PersistCurrentSnapshotAsync(
            processingDate,
            cancellationToken);
    }

    private async Task PersistCurrentSnapshotAsync(
        DateOnly processingDate,
        CancellationToken cancellationToken)
    {
        await _persistenceLock.WaitAsync(
            cancellationToken);

        try
        {
            var snapshot =
                _store.GetSnapshot();

            await _fileBackupStore.WriteAsync(
                processingDate,
                snapshot,
                cancellationToken);

            _logger.LogInformation(
                "[INQUIRY_SNAPSHOT_PERSISTED] " +
                "processingDate={ProcessingDate} " +
                "rows={RowCount}",
                processingDate,
                snapshot.Count);
        }
        finally
        {
            _persistenceLock.Release();
        }
    }

    private DateOnly GetProcessingDate()
    {
        return DateOnly.FromDateTime(
            DateTime.Now);
    }

    private int GetRetentionDays()
    {
        var configuredValue =
            _configuration[
                "MonitoringBackup:RetentionDays"];

        if (!int.TryParse(
                configuredValue,
                out var retentionDays))
        {
            throw new InvalidOperationException(
                "MonitoringBackup:RetentionDays " +
                "is not configured correctly.");
        }

        if (retentionDays < 0)
        {
            throw new InvalidOperationException(
                "MonitoringBackup:RetentionDays " +
                "cannot be negative.");
        }

        return retentionDays;
    }

    private static DateOnly GetTglProsesDate(
        long tglProses)
    {
        if (tglProses <= 0)
        {
            return DateOnly.MinValue;
        }

        var value =
            tglProses.ToString();

        if (value.Length < 8)
        {
            return DateOnly.MinValue;
        }

        var dateText =
            value[..8];

        return DateOnly.TryParseExact(
                dateText,
                "yyyyMMdd",
                out var date)
            ? date
            : DateOnly.MinValue;
    }
}