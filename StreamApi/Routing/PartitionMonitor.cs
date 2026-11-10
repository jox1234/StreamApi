using Microsoft.Extensions.Logging;
using Npgsql;
using StreamApi.Configuration;
using StreamApi.PostgreSQL;
using StreamApi.Processing;
using StreamApi.Processing.Inquiry;
using StreamApi.Processing.TableMapping;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace StreamApi.Routing;

public sealed class PartitionMonitor
{
    private readonly PartitionRoute _route;
    private readonly PostgreSqlConnectionFactory _connectionFactory;
    private readonly PostgreSqlDatabaseInitializer _databaseInitializer;
    private readonly PartitionConnectionBudget _connectionBudget;
    private readonly ProcessingEventQueue _processingQueue;
    private readonly ProcessingEventTracker _processingEventTracker;
    private readonly TableRegistry _tableRegistry;
    private readonly InquiryWorker _inquiryWorker;
    private readonly ILogger<PartitionMonitor> _logger;

    private readonly object _sync = new();

    private CancellationTokenSource? _monitorCancellation;
    private Task? _monitorTask;
    private NpgsqlConnection? _connection;
    private IDisposable? _connectionLease;

    public PartitionMonitor(
        PartitionRoute route,
        PostgreSqlConnectionFactory connectionFactory,
        PostgreSqlDatabaseInitializer databaseInitializer,
        PartitionConnectionBudget connectionBudget,
        ProcessingEventQueue processingQueue,
        ProcessingEventTracker processingEventTracker,
        TableRegistry tableRegistry,
        InquiryWorker inquiryWorker,
        ILogger<PartitionMonitor> logger)
    {
        _route = route;
        _connectionFactory = connectionFactory;
        _databaseInitializer = databaseInitializer;
        _connectionBudget = connectionBudget;
        _processingQueue = processingQueue;
        _processingEventTracker = processingEventTracker;
        _tableRegistry = tableRegistry;
        _inquiryWorker = inquiryWorker;
        _logger = logger;
    }

    public int PartitionId =>
        _route.PartitionId;

    public string DatabaseName =>
        _route.DatabaseName;

    public int TokoCount =>
        _route.TokoCount;

    public PartitionMonitorState State { get; private set; } =
        PartitionMonitorState.Stopped;

    public async Task StartAsync(
        CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (State is
                PartitionMonitorState.Running or
                PartitionMonitorState.Starting)
            {
                return;
            }

            State = PartitionMonitorState.Starting;
        }

        _logger.LogInformation(
            "[PARTITION_MONITOR_START] partition={PartitionId} database={Database} toko={TokoCount}",
            _route.PartitionId,
            _route.DatabaseName,
            _route.TokoCount);

        IDisposable? connectionLease = null;
        NpgsqlConnection? connection = null;

        try
        {
            /*
             * Acquire the PostgreSQL connection budget
             * before opening a persistent partition connection.
             */
            lock (_sync)
            {
                State = PartitionMonitorState.WaitingForConnection;
            }

            _logger.LogDebug(
                "[PARTITION_MONITOR_WAITING_FOR_CONNECTION] partition={PartitionId} database={Database}",
                _route.PartitionId,
                _route.DatabaseName);

            var opened =
                await OpenConnectionWithRetryAsync(
                    cancellationToken);

            connection = opened.Connection;
            connectionLease = opened.Lease;

            /*
             * Check whether this partition actually has
             * log_dt3_proses before attempting trigger installation.
             */
            var triggerInstalled =
                await _databaseInitializer.EnsureLogDt3TriggerAsync(
                    connection,
                    cancellationToken);

            if (!triggerInstalled)
            {
                _logger.LogInformation(
                    "[PARTITION_MONITOR_UNSUPPORTED] partition={PartitionId} database={Database} reason=log_dt3_proses_missing",
                    _route.PartitionId,
                    _route.DatabaseName);

                await connection.CloseAsync();
                await connection.DisposeAsync();

                connection = null;

                connectionLease.Dispose();
                connectionLease = null;

                State = PartitionMonitorState.Unsupported;

                return;
            }

            await using (var command = new NpgsqlCommand(
                $"LISTEN {PostgreSqlTriggerDefinition.ChannelName};",
                connection))
            {
                await command.ExecuteNonQueryAsync(
                    cancellationToken);
            }

            connection.Notification += OnNotification;

            lock (_sync)
            {
                _connection = connection;
                _connectionLease = connectionLease;
            }

            connection = null;
            connectionLease = null;

            _monitorCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);

            State = PartitionMonitorState.Running;

            _logger.LogInformation(
                "[PARTITION_MONITOR_RUNNING] partition={PartitionId} database={Database} channel={Channel}",
                _route.PartitionId,
                _route.DatabaseName,
                PostgreSqlTriggerDefinition.ChannelName);

            _monitorTask =
                MonitorLoopAsync(
                    _monitorCancellation.Token);
        }
        catch
        {
            if (connection is not null)
            {
                try
                {
                    connection.Notification -= OnNotification;
                    await connection.CloseAsync();
                }
                catch
                {
                    // Ignore cleanup failure.
                }

                await connection.DisposeAsync();
            }

            connectionLease?.Dispose();

            lock (_sync)
            {
                _connection = null;
                _connectionLease = null;
            }

            State = PartitionMonitorState.Failed;

            throw;
        }
    }

    public async Task StopAsync(
        CancellationToken cancellationToken)
    {
        Task? monitorTask;
        NpgsqlConnection? connection;
        CancellationTokenSource? monitorCancellation;
        IDisposable? connectionLease;

        lock (_sync)
        {
            if (State == PartitionMonitorState.Stopped)
            {
                return;
            }

            State = PartitionMonitorState.Stopping;

            monitorTask = _monitorTask;
            connection = _connection;
            monitorCancellation = _monitorCancellation;
            connectionLease = _connectionLease;

            _monitorTask = null;
            _connection = null;
            _monitorCancellation = null;
            _connectionLease = null;
        }

        _logger.LogInformation(
            "[PARTITION_MONITOR_STOP] partition={PartitionId} database={Database}",
            _route.PartitionId,
            _route.DatabaseName);

        try
        {
            monitorCancellation?.Cancel();

            if (connection is not null)
            {
                connection.Notification -= OnNotification;
            }

            if (monitorTask is not null)
            {
                try
                {
                    await monitorTask.WaitAsync(
                        cancellationToken);
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    // Shutdown cancellation.
                }
            }

            if (connection is not null)
            {
                try
                {
                    await connection.CloseAsync();
                }
                catch
                {
                    // Connection may already be broken.
                }

                await connection.DisposeAsync();
            }
        }
        finally
        {
            connectionLease?.Dispose();
            monitorCancellation?.Dispose();

            State = PartitionMonitorState.Stopped;
        }
    }

    private async Task<(NpgsqlConnection Connection, IDisposable Lease)>
        OpenConnectionWithRetryAsync(
            CancellationToken cancellationToken)
    {
        var delay =
            TimeSpan.FromSeconds(1);

        while (!cancellationToken.IsCancellationRequested)
        {
            IDisposable? lease = null;
            NpgsqlConnection? connection = null;

            try
            {
                lease =
                    await _connectionBudget.TryAcquireMonitorAsync(
                        TimeSpan.FromSeconds(30),
                        cancellationToken);

                /*
                 * The connection budget may be temporarily full.
                 * Do not create a PostgreSQL connection until
                 * a budget slot has actually been acquired.
                 */
                if (lease is null)
                {
                    _logger.LogDebug(
                        "[PARTITION_MONITOR_CONNECTION_CAPACITY_WAIT] partition={PartitionId} database={Database} capacity={Capacity} active={Active} monitorCapacity={MonitorCapacity} activeMonitors={ActiveMonitors} retryInSeconds={RetrySeconds}",
                        _route.PartitionId,
                        _route.DatabaseName,
                        _connectionBudget.Capacity,
                        _connectionBudget.ActiveConnections,
                        _connectionBudget.MonitorCapacity,
                        _connectionBudget.ActiveMonitorConnections,
                        delay.TotalSeconds);

                    await Task.Delay(
                        delay,
                        cancellationToken);

                    delay =
                        TimeSpan.FromSeconds(
                            Math.Min(
                                delay.TotalSeconds * 2,
                                30));

                    continue;
                }

                _logger.LogDebug(
                    "[PARTITION_MONITOR_CONNECTION_SLOT_ACQUIRED] partition={PartitionId} database={Database} active={Active} capacity={Capacity} activeMonitors={ActiveMonitors} monitorCapacity={MonitorCapacity}",
                    _route.PartitionId,
                    _route.DatabaseName,
                    _connectionBudget.ActiveConnections,
                    _connectionBudget.Capacity,
                    _connectionBudget.ActiveMonitorConnections,
                    _connectionBudget.MonitorCapacity);

                connection =
                    _connectionFactory.CreateConnection(
                        _route.DatabaseName);

                await connection.OpenAsync(
                    cancellationToken);

                /*
                 * PostgreSQL connection succeeded.
                 * Reset the retry delay so a future reconnect
                 * starts conservatively from one second.
                 */
                delay =
                    TimeSpan.FromSeconds(1);

                return (
                    connection,
                    lease);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                /*
                 * The initial connection attempt was cancelled.
                 *
                 * The lease was already acquired, so it must be
                 * released before propagating cancellation.
                 */
                if (connection is not null)
                {
                    try
                    {
                        await connection.CloseAsync();
                    }
                    catch
                    {
                        // Ignore cleanup failure.
                    }

                    await connection.DisposeAsync();
                    connection = null;
                }

                lease?.Dispose();
                lease = null;

                throw;
            }
            catch (PostgresException ex)
                when (ex.SqlState == "53300")
            {
                /*
                 * PostgreSQL rejected the connection because the
                 * server connection capacity was reached.
                 *
                 * Release both resources before retrying.
                 */
                if (connection is not null)
                {
                    await connection.DisposeAsync();
                    connection = null;
                }

                lease?.Dispose();
                lease = null;

                _logger.LogWarning(
                    "[PARTITION_MONITOR_CONNECTION_CAPACITY] partition={PartitionId} database={Database} retryInSeconds={RetrySeconds} active={Active} capacity={Capacity}",
                    _route.PartitionId,
                    _route.DatabaseName,
                    delay.TotalSeconds,
                    _connectionBudget.ActiveConnections,
                    _connectionBudget.Capacity);

                await Task.Delay(
                    delay,
                    cancellationToken);

                delay =
                    TimeSpan.FromSeconds(
                        Math.Min(
                            delay.TotalSeconds * 2,
                            30));
            }
            catch (Exception ex)
            {
                /*
                 * Any other connection/open failure must release
                 * the acquired monitor lease before retrying.
                 *
                 * Without this cleanup, a failed OpenAsync()
                 * could permanently consume a connection-budget
                 * slot and eventually make the monitor budget appear
                 * exhausted even though no connection is active.
                 */
                if (connection is not null)
                {
                    try
                    {
                        await connection.CloseAsync();
                    }
                    catch
                    {
                        // Ignore cleanup failure.
                    }

                    await connection.DisposeAsync();
                    connection = null;
                }

                lease?.Dispose();
                lease = null;

                _logger.LogWarning(
                    ex,
                    "[PARTITION_MONITOR_CONNECTION_FAILED] partition={PartitionId} database={Database} retryInSeconds={RetrySeconds}",
                    _route.PartitionId,
                    _route.DatabaseName,
                    delay.TotalSeconds);

                await Task.Delay(
                    delay,
                    cancellationToken);

                delay =
                    TimeSpan.FromSeconds(
                        Math.Min(
                            delay.TotalSeconds * 2,
                            30));
            }
        }

        throw new OperationCanceledException(
            cancellationToken);
    }

    private async Task MonitorLoopAsync(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            NpgsqlConnection? connection;

            lock (_sync)
            {
                connection = _connection;
            }

            if (connection is null)
            {
                return;
            }

            try
            {
                await connection.WaitAsync(
                    cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "[PARTITION_MONITOR_CONNECTION_LOST] partition={PartitionId} database={Database}",
                    _route.PartitionId,
                    _route.DatabaseName);

                await ReconnectAsync(
                    cancellationToken);
            }
        }
    }

    private async Task ReconnectAsync(
        CancellationToken cancellationToken)
    {
        State = PartitionMonitorState.Reconnecting;

        _logger.LogInformation(
            "[PARTITION_MONITOR_RECONNECT_START] partition={PartitionId} database={Database}",
            _route.PartitionId,
            _route.DatabaseName);

        var delay =
            TimeSpan.FromSeconds(1);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                NpgsqlConnection? oldConnection;
                IDisposable? oldLease;

                lock (_sync)
                {
                    oldConnection = _connection;
                    oldLease = _connectionLease;

                    _connection = null;
                    _connectionLease = null;
                }

                if (oldConnection is not null)
                {
                    oldConnection.Notification -= OnNotification;

                    try
                    {
                        await oldConnection.CloseAsync();
                    }
                    catch
                    {
                        // Connection is already broken.
                    }

                    await oldConnection.DisposeAsync();
                }

                /*
                 * Release the old connection slot before
                 * attempting to acquire a new one.
                 */
                oldLease?.Dispose();

                /*
                 * Re-acquire a connection slot.
                 */
                var newLease =
                    await _connectionBudget.AcquireMonitorAsync(
                        cancellationToken);

                NpgsqlConnection? newConnection = null;

                try
                {
                    newConnection =
                        _connectionFactory.CreateConnection(
                            _route.DatabaseName);

                    await newConnection.OpenAsync(
                        cancellationToken);

                    var triggerInstalled =
                        await _databaseInitializer.EnsureLogDt3TriggerAsync(
                            newConnection,
                            cancellationToken);

                    if (!triggerInstalled)
                    {
                        _logger.LogInformation(
                            "[PARTITION_MONITOR_UNSUPPORTED] partition={PartitionId} database={Database} reason=log_dt3_proses_missing",
                            _route.PartitionId,
                            _route.DatabaseName);

                        await newConnection.CloseAsync();
                        await newConnection.DisposeAsync();

                        newConnection = null;

                        newLease.Dispose();

                        State = PartitionMonitorState.Unsupported;

                        return;
                    }

                    await using (var command = new NpgsqlCommand(
                        $"LISTEN {PostgreSqlTriggerDefinition.ChannelName};",
                        newConnection))
                    {
                        await command.ExecuteNonQueryAsync(
                            cancellationToken);
                    }

                    newConnection.Notification += OnNotification;

                    lock (_sync)
                    {
                        _connection = newConnection;
                        _connectionLease = newLease;
                    }

                    newConnection = null;

                    State = PartitionMonitorState.Running;

                    _logger.LogInformation(
                        "[PARTITION_MONITOR_RECONNECTED] partition={PartitionId} database={Database}",
                        _route.PartitionId,
                        _route.DatabaseName);

                    return;
                }
                catch
                {
                    if (newConnection is not null)
                    {
                        try
                        {
                            newConnection.Notification -= OnNotification;
                            await newConnection.CloseAsync();
                        }
                        catch
                        {
                            // Ignore cleanup failure.
                        }

                        await newConnection.DisposeAsync();
                    }

                    newLease.Dispose();

                    throw;
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "[PARTITION_MONITOR_RECONNECT_FAILED] partition={PartitionId} database={Database} retryInSeconds={RetrySeconds}",
                    _route.PartitionId,
                    _route.DatabaseName,
                    delay.TotalSeconds);

                await Task.Delay(
                    delay,
                    cancellationToken);

                delay =
                    TimeSpan.FromSeconds(
                        Math.Min(
                            delay.TotalSeconds * 2,
                            30));
            }
        }
    }

    private void OnNotification(
        object sender,
        NpgsqlNotificationEventArgs e)
    {
        if (!string.Equals(
                e.Channel,
                PostgreSqlTriggerDefinition.ChannelName,
                StringComparison.Ordinal))
        {
            return;
        }

        try
        {
            var payload =
                JsonSerializer.Deserialize<LogDt3Notification>(
                    e.Payload);

            if (payload is null)
            {
                _logger.LogWarning(
                    "[PARTITION_MONITOR_INVALID_NOTIFICATION] partition={PartitionId} database={Database} reason=empty_payload",
                    _route.PartitionId,
                    _route.DatabaseName);

                return;
            }

            if (string.IsNullOrWhiteSpace(
                    payload.KodeToko))
            {
                _logger.LogWarning(
                    "[PARTITION_MONITOR_INVALID_NOTIFICATION] partition={PartitionId} database={Database} reason=missing_kode_toko",
                    _route.PartitionId,
                    _route.DatabaseName);

                return;
            }

            if (string.IsNullOrWhiteSpace(
                    payload.JenisFile))
            {
                _logger.LogWarning(
                    "[PARTITION_MONITOR_INVALID_NOTIFICATION] partition={PartitionId} database={Database} reason=missing_jenis_file",
                    _route.PartitionId,
                    _route.DatabaseName);

                return;
            }

            if (payload.TglProses <= 0)
            {
                _logger.LogWarning(
                    "[PARTITION_MONITOR_INVALID_NOTIFICATION] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} reason=invalid_tgl_proses",
                    _route.PartitionId,
                    _route.DatabaseName,
                    payload.KodeToko,
                    payload.JenisFile);

                return;
            }

            if (payload.JumlahRecord < 0)
            {
                _logger.LogWarning(
                    "[PARTITION_MONITOR_INVALID_NOTIFICATION] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} reason=invalid_jumlah_record",
                    _route.PartitionId,
                    _route.DatabaseName,
                    payload.KodeToko,
                    payload.JenisFile,
                    payload.TglProses);

                return;
            }

            _logger.LogInformation(
                "[LOG_DT3_NOTIFICATION] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglDt3={TglDt3} jumlahRecord={JumlahRecord} tglProses={TglProses}",
                _route.PartitionId,
                _route.DatabaseName,
                payload.KodeToko,
                payload.JenisFile,
                payload.TglDt3,
                payload.JumlahRecord,
                payload.TglProses);

            /*
             * Inquiry receives EVERY valid log_dt3_proses
             * notification.
             *
             * This intentionally happens BEFORE the
             * tables.json/TableRegistry filter below.
             *
             * Inquiry is not controlled by tables.json.
             */
            var inquiryRow =
                new LogDt3Row(
                    KodeToko:
                        payload.KodeToko,
                    JenisFile:
                        payload.JenisFile,
                    TglDt3:
                        payload.TglDt3,
                    JumlahRecord:
                        payload.JumlahRecord,
                    TglProses:
                        payload.TglProses);

            if (!_inquiryWorker.TryEnqueue(
                    inquiryRow))
            {
                _logger.LogWarning(
                    "[INQUIRY_NOTIFICATION_QUEUE_FULL] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                    _route.PartitionId,
                    _route.DatabaseName,
                    payload.KodeToko,
                    payload.JenisFile,
                    payload.TglProses);
            }
            else
            {
                _logger.LogDebug(
                    "[INQUIRY_NOTIFICATION_ENQUEUED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                    _route.PartitionId,
                    _route.DatabaseName,
                    payload.KodeToko,
                    payload.JenisFile,
                    payload.TglProses);
            }

            var reconciliationEvent =
                new ReconciliationEvent(
                    PartitionId:
                        _route.PartitionId,

                    DatabaseName:
                        _route.DatabaseName,

                    KodeToko:
                        payload.KodeToko,

                    JenisFile:
                        payload.JenisFile,

                    TglDt3:
                        payload.TglDt3,

                    JumlahRecord:
                        payload.JumlahRecord,

                    TglProses:
                        payload.TglProses);

            /*
             * Apply the same table configuration rules used by
             * reconciliation before claiming the event.
             *
             * Inquiry has already received the event above,
             * so this filter only controls normal processing.
             */
            if (!_tableRegistry.TryGet(
                    reconciliationEvent.JenisFile,
                    out var tableConfiguration)
                || tableConfiguration is null)
            {
                _logger.LogDebug(
                    "[LOG_DT3_NOTIFICATION_SKIPPED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} reason=table_not_configured",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses);

                return;
            }

            if (!tableConfiguration.Enabled)
            {
                _logger.LogDebug(
                    "[LOG_DT3_NOTIFICATION_SKIPPED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} reason=table_disabled",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses);

                return;
            }

            /*
             * Prevent the same event from being simultaneously
             * claimed by LISTEN/NOTIFY and reconciliation.
             */
            if (!_processingEventTracker.TryClaim(
                    reconciliationEvent))
            {
                _logger.LogDebug(
                    "[LOG_DT3_NOTIFICATION_ALREADY_TRACKED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses);

                return;
            }

            /*
             * The notification callback must not block waiting
             * for the bounded processing queue.
             *
             * If the queue is full, reconciliation will discover
             * the event during its normal recovery scan.
             */
            if (!_processingQueue.TryEnqueue(
                    reconciliationEvent))
            {
                _processingEventTracker.Release(
                    reconciliationEvent);

                _logger.LogWarning(
                    "[LOG_DT3_NOTIFICATION_QUEUE_FULL] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} pending={Pending}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses,
                    _processingQueue.Pending);

                return;
            }

            _logger.LogInformation(
                "[LOG_DT3_NOTIFICATION_ENQUEUED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglDt3={TglDt3} jumlahRecord={JumlahRecord} tglProses={TglProses} pending={Pending}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglDt3,
                reconciliationEvent.JumlahRecord,
                reconciliationEvent.TglProses,
                _processingQueue.Pending);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "[PARTITION_MONITOR_INVALID_NOTIFICATION] partition={PartitionId} database={Database} reason=invalid_json",
                _route.PartitionId,
                _route.DatabaseName);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[PARTITION_MONITOR_NOTIFICATION_FAILED] partition={PartitionId} database={Database}",
                _route.PartitionId,
                _route.DatabaseName);
        }
    }

    public PartitionMonitorSnapshot GetSnapshot()
    {
        lock (_sync)
        {
            return new PartitionMonitorSnapshot(
                _route.PartitionId,
                _route.DatabaseName,
                _route.TokoCount,
                _route.TokoRoutes.ToArray(),
                State);
        }
    }

    private sealed class LogDt3Notification
    {
        [JsonPropertyName("kode_toko")]
        public string? KodeToko { get; init; }

        [JsonPropertyName("jenis_file")]
        public string? JenisFile { get; init; }

        [JsonPropertyName("tgl_dt3")]
        public DateOnly TglDt3 { get; init; }

        [JsonPropertyName("jumlah_record")]
        public int JumlahRecord { get; init; }

        [JsonPropertyName("tgl_proses")]
        public long TglProses { get; init; }
    }
}