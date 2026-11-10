using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using StreamApi.Configuration;
using StreamApi.PostgreSQL;
using StreamApi.Processing;

namespace StreamApi.Routing;

public sealed class ReconciliationScheduler
    : BackgroundService
{
    private static readonly TimeSpan ReconciliationInterval =
        TimeSpan.FromMinutes(1);

    private readonly ILogger<ReconciliationScheduler>
        _logger;

    private readonly PartitionMonitorManager
        _partitionMonitorManager;

    private readonly PostgreSqlConnectionFactory
        _connectionFactory;

    private readonly PostgreSqlLogDt3Reader
        _reader;

    private readonly ReconciliationCheckpointStore
        _checkpointStore;

    private readonly ProcessingEventQueue
        _processingQueue;

    private readonly PartitionConnectionBudget
        _connectionBudget;

    private readonly ProcessingEventTracker
        _processingEventTracker;

    private readonly TableRegistry
        _tableRegistry;

    public ReconciliationScheduler(
        ILogger<ReconciliationScheduler> logger,
        PartitionMonitorManager partitionMonitorManager,
        PostgreSqlConnectionFactory connectionFactory,
        PostgreSqlLogDt3Reader reader,
        ReconciliationCheckpointStore checkpointStore,
        ProcessingEventQueue processingQueue,
        PartitionConnectionBudget connectionBudget,
        ProcessingEventTracker processingEventTracker,
        TableRegistry tableRegistry)
    {
        _logger =
            logger;

        _partitionMonitorManager =
            partitionMonitorManager;

        _connectionFactory =
            connectionFactory;

        _reader =
            reader;

        _checkpointStore =
            checkpointStore;

        _processingQueue =
            processingQueue;

        _connectionBudget =
            connectionBudget;

        _processingEventTracker =
            processingEventTracker;

        _tableRegistry =
            tableRegistry;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "[RECONCILIATION_START] Reconciliation scheduler started.");

        /*
         * First-start checkpoint baseline.
         *
         * This prevents the first deployment from replaying the
         * entire historical log_dt3_proses history.
         *
         * Once the baseline has been initialized, this block is
         * skipped forever because the initialization state is
         * persisted in POSCatalog_Beta.
         */
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await InitializeCheckpointBaselineAsync(
                    stoppingToken);

                if (_checkpointStore.IsInitialized)
                {
                    break;
                }

                await Task.Delay(
                    TimeSpan.FromSeconds(1),
                    stoppingToken);
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[CHECKPOINT_BASELINE_STARTUP_FAILED] " +
                "Checkpoint baseline initialization failed.");

            /*
             * Do not terminate the scheduler.
             *
             * The next loop iteration will retry baseline
             * initialization.
             */
        }

        if (stoppingToken.IsCancellationRequested)
        {
            return;
        }

        /*
         * Perform one reconciliation immediately after the
         * checkpoint baseline has been established.
         */
        try
        {
            await RunReconciliationAsync(
                stoppingToken);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            /*
             * One failed startup reconciliation must not kill
             * the BackgroundService permanently.
             */
            _logger.LogError(
                ex,
                "[RECONCILIATION_STARTUP_FAILED] " +
                "Initial reconciliation failed.");
        }

        _logger.LogInformation(
            "[RECONCILIATION_INITIAL_COMPLETE] " +
            "Initial reconciliation finished; starting periodic scheduler.");

        _logger.LogInformation(
            "[RECONCILIATION_TIMER_CREATED] " +
            "intervalSeconds={IntervalSeconds}",
            ReconciliationInterval.TotalSeconds);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogDebug(
                    "[RECONCILIATION_TIMER_WAITING] " +
                    "next reconciliation in {IntervalSeconds} seconds.",
                    ReconciliationInterval.TotalSeconds);

                try
                {
                    await Task.Delay(
                        ReconciliationInterval,
                        stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "[RECONCILIATION_TIMER_WAIT_FAILED] " +
                        "Periodic delay failed.");

                    /*
                     * Do not permanently terminate the reconciliation
                     * scheduler because of an unexpected timing failure.
                     * Continue with the next scheduler iteration.
                     */
                    continue;
                }

                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                _logger.LogInformation(
                    "[RECONCILIATION_TIMER_TICK] " +
                    "Running scheduled reconciliation cycle.");

                try
                {
                    await RunReconciliationAsync(
                        stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    /*
                     * A single reconciliation cycle must not
                     * terminate the scheduler permanently.
                     */
                    _logger.LogError(
                        ex,
                        "[RECONCILIATION_CYCLE_FAILED] " +
                        "Reconciliation cycle failed.");
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Normal application shutdown.
        }
        catch (Exception ex)
        {
            /*
             * Last-resort protection for the scheduler itself.
             */
            _logger.LogCritical(
                ex,
                "[RECONCILIATION_SCHEDULER_FAILED] " +
                "Unexpected scheduler failure.");
        }
        finally
        {
            _logger.LogInformation(
                "[RECONCILIATION_STOP] " +
                "Reconciliation scheduler stopped.");
        }
    }

    private async Task InitializeCheckpointBaselineAsync(
        CancellationToken cancellationToken)
    {
        if (_checkpointStore.IsInitialized)
        {
            return;
        }

        var initialized =
            await _checkpointStore
                .IsInitializedAsync(
                    cancellationToken);

        if (initialized)
        {
            _checkpointStore.MarkInitialized();

            _logger.LogInformation(
                "[CHECKPOINT_BASELINE_ALREADY_INITIALIZED]");

            return;
        }

        var snapshots =
            _partitionMonitorManager
                .GetMonitorSnapshots();

        if (snapshots.Count == 0)
        {
            _logger.LogInformation(
                "[CHECKPOINT_BASELINE_WAITING_FOR_ROUTING] " +
                "no partition monitors are available yet.");

            return;
        }

        _logger.LogInformation(
            "[CHECKPOINT_BASELINE_START] " +
            "partitions={PartitionCount}",
            snapshots.Count);

        var successfulPartitions = 0;
        var failedPartitions = 0;
        var totalCheckpointRows = 0;

        foreach (var partition in snapshots)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IDisposable? connectionLease = null;
            NpgsqlConnection? connection = null;

            var connectionSlotAcquired =
                false;

            try
            {
                _logger.LogDebug(
                    "[CHECKPOINT_BASELINE_WAITING_FOR_CONNECTION] " +
                    "partition={PartitionId} database={Database}",
                    partition.PartitionId,
                    partition.DatabaseName);

                connectionLease =
                    await _connectionBudget.AcquireAsync(
                        cancellationToken);

                connectionSlotAcquired =
                    true;

                connection =
                    _connectionFactory.CreateConnection(
                        partition.DatabaseName);

                await connection.OpenAsync(
                    cancellationToken);

                _logger.LogDebug(
                    "[CHECKPOINT_BASELINE_CONNECTION_OPENED] " +
                    "partition={PartitionId} database={Database}",
                    partition.PartitionId,
                    connection.Database);

                var latestRows =
                    await _reader.ReadLatestRowsAsync(
                        connection,
                        partition,
                        cancellationToken);

                foreach (var row in latestRows)
                {
                    await _checkpointStore
                        .SeedBaselineAsync(
                            row.KodeToko,
                            row.JenisFile,
                            row.TglProses,
                            cancellationToken);

                    totalCheckpointRows++;
                }

                successfulPartitions++;

                _logger.LogDebug(
                    "[CHECKPOINT_BASELINE_PARTITION_COMPLETE] " +
                    "partition={PartitionId} " +
                    "database={Database} " +
                    "latestEvents={LatestEvents}",
                    partition.PartitionId,
                    partition.DatabaseName,
                    latestRows.Count);
            }
            catch (PostgresException ex)
            {
                failedPartitions++;

                _logger.LogWarning(
                    ex,
                    "[CHECKPOINT_BASELINE_DATABASE_FAILED] " +
                    "partition={PartitionId} " +
                    "database={Database} " +
                    "sqlState={SqlState}",
                    partition.PartitionId,
                    partition.DatabaseName,
                    ex.SqlState);
            }
            catch (Exception ex)
            {
                failedPartitions++;

                _logger.LogWarning(
                    ex,
                    "[CHECKPOINT_BASELINE_FAILED] " +
                    "partition={PartitionId} " +
                    "database={Database}",
                    partition.PartitionId,
                    partition.DatabaseName);
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

                if (connectionSlotAcquired)
                {
                    connectionLease?.Dispose();
                }
            }
        }

        /*
         * IMPORTANT:
         *
         * We only mark the baseline initialized after every
         * partition has been attempted.
         *
         * If some partitions failed because they were temporarily
         * unavailable, we DO NOT permanently declare the baseline
         * complete. The next startup attempt/cycle can retry them.
         */
        if (failedPartitions > 0)
        {
            _logger.LogWarning(
                "[CHECKPOINT_BASELINE_INCOMPLETE] " +
                "partitions={PartitionCount} " +
                "successfulPartitions={SuccessfulPartitions} " +
                "failedPartitions={FailedPartitions} " +
                "checkpointRows={CheckpointRows}",
                snapshots.Count,
                successfulPartitions,
                failedPartitions,
                totalCheckpointRows);

            return;
        }

        await _checkpointStore
            .MarkInitializedAsync(
                cancellationToken);

        _logger.LogInformation(
            "[CHECKPOINT_BASELINE_COMPLETE] " +
            "partitions={PartitionCount} " +
            "successfulPartitions={SuccessfulPartitions} " +
            "failedPartitions={FailedPartitions} " +
            "checkpointRows={CheckpointRows}",
            snapshots.Count,
            successfulPartitions,
            failedPartitions,
            totalCheckpointRows);
    }

    private async Task RunReconciliationAsync(
        CancellationToken cancellationToken)
    {
        var startedAt =
            Stopwatch.GetTimestamp();

        /*
         * IMPORTANT DATABASE MODEL
         *
         * log_dt3_proses:
         *     lives in each POSIDM_xxx partition database.
         *
         * streamapi_processing_checkpoint:
         *     lives in central POSCatalog_Beta.
         *
         * Therefore:
         *
         *     partition connection
         *          |
         *          +--> read log_dt3_proses
         *
         *     central catalog connection
         *          |
         *          +--> checkpoint state
         *
         * Never attempt to read the checkpoint table from the
         * partition database.
         */

        /*
         * This snapshot is intentionally used as the read snapshot
         * for this reconciliation cycle.
         *
         * IMPORTANT:
         *
         * The snapshot can become stale while processing is running.
         * Therefore every event is revalidated against the CURRENT
         * checkpoint immediately before enqueueing below.
         */
        var checkpoints =
            _checkpointStore.Snapshot();

        var snapshots =
            _partitionMonitorManager
                .GetMonitorSnapshots();

        var candidates =
            snapshots
                .Where(IsReconciliationCandidate)
                .ToArray();

        _logger.LogInformation(
            "[RECONCILIATION_CYCLE_START] " +
            "monitors={MonitorCount} " +
            "candidates={CandidateCount} " +
            "checkpoints={CheckpointCount}",
            snapshots.Count,
            candidates.Length,
            checkpoints.Count);

        /*
         * Every partition task participates in the same global
         * PostgreSQL connection budget.
         */
        var tasks =
            candidates.Select(
                partition =>
                    ReconcilePartitionAsync(
                        partition,
                        checkpoints,
                        cancellationToken));

        await Task.WhenAll(
            tasks);

        var elapsed =
            Stopwatch.GetElapsedTime(
                startedAt);

        _logger.LogInformation(
            "[RECONCILIATION_CYCLE_COMPLETE] " +
            "candidates={CandidateCount} " +
            "elapsedMs={ElapsedMs}",
            candidates.Length,
            elapsed.TotalMilliseconds);
    }

    private static bool IsReconciliationCandidate(
        PartitionMonitorSnapshot snapshot)
    {
        return snapshot.State is
            PartitionMonitorState.Running
            or PartitionMonitorState.WaitingForConnection
            or PartitionMonitorState.Reconnecting
            or PartitionMonitorState.Failed;
    }

    private async Task ReconcilePartitionAsync(
        PartitionMonitorSnapshot partition,
        IReadOnlyDictionary<
            ReconciliationCheckpointKey,
            long> checkpoints,
        CancellationToken cancellationToken)
    {
        IDisposable? connectionLease = null;
        NpgsqlConnection? connection = null;

        var connectionSlotAcquired =
            false;

        try
        {
            _logger.LogDebug(
                "[RECONCILIATION_WAITING_FOR_CONNECTION] " +
                "partition={PartitionId} database={Database}",
                partition.PartitionId,
                partition.DatabaseName);

            connectionLease =
                await _connectionBudget.AcquireAsync(
                    cancellationToken);

            connectionSlotAcquired =
                true;

            _logger.LogDebug(
                "[RECONCILIATION_CONNECTION_SLOT_ACQUIRED] " +
                "partition={PartitionId} database={Database}",
                partition.PartitionId,
                partition.DatabaseName);

            connection =
                _connectionFactory.CreateConnection(
                    partition.DatabaseName);

            await connection.OpenAsync(
                cancellationToken);

            _logger.LogDebug(
                "[RECONCILIATION_CONNECTION_OPENED] " +
                "partition={PartitionId} database={Database}",
                partition.PartitionId,
                connection.Database);

            var rows =
                await _reader.ReadNewRowsAsync(
                    connection,
                    partition,
                    checkpoints,
                    cancellationToken);

            var newRows =
                rows
                    .Where(row =>
                        IsNewEvent(
                            row,
                            checkpoints))
                    .ToArray();

            _logger.LogInformation(
                "[RECONCILIATION_DETECTED] " +
                "partition={PartitionId} " +
                "database={Database} " +
                "candidates={Candidates} " +
                "new={New}",
                partition.PartitionId,
                partition.DatabaseName,
                rows.Count,
                newRows.Length);

            var processableRows =
                new List<LogDt3Row>(
                    newRows.Length);

            var skippedRows =
                0;

            foreach (var row in newRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!_tableRegistry.TryGet(
                        row.JenisFile,
                        out var tableConfiguration)
                    || tableConfiguration is null)
                {
                    skippedRows++;

                    _logger.LogDebug(
                        "[RECONCILIATION_EVENT_SKIPPED] " +
                        "reason=table_not_configured " +
                        "partition={PartitionId} " +
                        "database={Database} " +
                        "toko={KodeToko} " +
                        "jenisFile={JenisFile} " +
                        "tglProses={TglProses}",
                        partition.PartitionId,
                        partition.DatabaseName,
                        row.KodeToko,
                        row.JenisFile,
                        row.TglProses);

                    continue;
                }

                if (!tableConfiguration.Enabled)
                {
                    skippedRows++;

                    _logger.LogDebug(
                        "[RECONCILIATION_EVENT_SKIPPED] " +
                        "reason=table_disabled " +
                        "partition={PartitionId} " +
                        "database={Database} " +
                        "toko={KodeToko} " +
                        "jenisFile={JenisFile} " +
                        "tglProses={TglProses}",
                        partition.PartitionId,
                        partition.DatabaseName,
                        row.KodeToko,
                        row.JenisFile,
                        row.TglProses);

                    continue;
                }

                processableRows.Add(row);
            }

            _logger.LogInformation(
                "[RECONCILIATION_CONFIGURATION_FILTER] " +
                "partition={PartitionId} " +
                "database={Database} " +
                "new={New} " +
                "processable={Processable} " +
                "skipped={Skipped}",
                partition.PartitionId,
                partition.DatabaseName,
                newRows.Length,
                processableRows.Count,
                skippedRows);

            foreach (var row in processableRows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var reconciliationEvent =
                    new ReconciliationEvent(
                        PartitionId:
                            partition.PartitionId,

                        DatabaseName:
                            partition.DatabaseName,

                        KodeToko:
                            row.KodeToko,

                        JenisFile:
                            row.JenisFile,

                        TglDt3:
                            row.TglDt3,

                        JumlahRecord:
                            row.JumlahRecord,

                        TglProses:
                            row.TglProses);

                _logger.LogInformation(
                    "[RECONCILIATION_EVENT_DETECTED] " +
                    "partition={PartitionId} " +
                    "database={Database} " +
                    "toko={KodeToko} " +
                    "jenisFile={JenisFile} " +
                    "tglDt3={TglDt3} " +
                    "jumlahRecord={JumlahRecord} " +
                    "tglProses={TglProses}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglDt3,
                    reconciliationEvent.JumlahRecord,
                    reconciliationEvent.TglProses);

                /*
                 * Claim the logical event first.
                 *
                 * This prevents multiple reconciliation paths inside
                 * the same process from simultaneously owning the same
                 * logical event.
                 */
                if (!_processingEventTracker.TryClaim(
                        reconciliationEvent))
                {
                    _logger.LogDebug(
                        "[RECONCILIATION_EVENT_ALREADY_TRACKED] " +
                        "partition={PartitionId} " +
                        "database={Database} " +
                        "toko={KodeToko} " +
                        "jenisFile={JenisFile} " +
                        "tglProses={TglProses}",
                        reconciliationEvent.PartitionId,
                        reconciliationEvent.DatabaseName,
                        reconciliationEvent.KodeToko,
                        reconciliationEvent.JenisFile,
                        reconciliationEvent.TglProses);

                    continue;
                }

                /*
                 * IMPORTANT RACE-PROTECTION:
                 *
                 * `checkpoints` is a snapshot taken at the start of
                 * the reconciliation cycle.
                 *
                 * Processing may have advanced the live checkpoint
                 * after that snapshot was created.
                 *
                 * Re-read the CURRENT in-memory checkpoint after the
                 * event has been claimed and immediately before it is
                 * placed onto the processing queue.
                 *
                 * If the event has already been processed, release the
                 * claim and do NOT enqueue it again.
                 *
                 * This prevents:
                 *
                 *     stale snapshot
                 *          -> event appears new
                 *          -> original processing completes
                 *          -> checkpoint advances
                 *          -> tracker releases
                 *          -> stale reconciliation reclaims event
                 *          -> event gets processed as a new lifecycle
                 */
                if (_checkpointStore.TryGet(
                        reconciliationEvent.KodeToko,
                        reconciliationEvent.JenisFile,
                        out var currentCheckpoint)
                    && reconciliationEvent.TglProses <=
                       currentCheckpoint)
                {
                    _processingEventTracker.Release(
                        reconciliationEvent);

                    _logger.LogInformation(
                        "[RECONCILIATION_EVENT_ALREADY_CHECKPOINTED] " +
                        "partition={PartitionId} " +
                        "database={Database} " +
                        "toko={KodeToko} " +
                        "jenisFile={JenisFile} " +
                        "tglProses={TglProses} " +
                        "currentCheckpoint={CurrentCheckpoint}",
                        reconciliationEvent.PartitionId,
                        reconciliationEvent.DatabaseName,
                        reconciliationEvent.KodeToko,
                        reconciliationEvent.JenisFile,
                        reconciliationEvent.TglProses,
                        currentCheckpoint);

                    continue;
                }

                try
                {
                    await _processingQueue.EnqueueAsync(
                        reconciliationEvent,
                        cancellationToken);
                }
                catch
                {
                    /*
                     * Queue insertion failed after claiming the event.
                     *
                     * Release the claim so a later reconciliation
                     * cycle can try again.
                     */
                    _processingEventTracker.Release(
                        reconciliationEvent);

                    throw;
                }

                _logger.LogDebug(
                    "[PROCESSING_QUEUE_ENQUEUED] " +
                    "partition={PartitionId} " +
                    "database={Database} " +
                    "toko={KodeToko} " +
                    "jenisFile={JenisFile} " +
                    "tglProses={TglProses}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses);
            }
        }
        catch (PostgresException ex)
        {
            _logger.LogWarning(
                ex,
                "[RECONCILIATION_DATABASE_FAILED] " +
                "partition={PartitionId} " +
                "database={Database} " +
                "sqlState={SqlState}",
                partition.PartitionId,
                partition.DatabaseName,
                ex.SqlState);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "[RECONCILIATION_FAILED] " +
                "partition={PartitionId} " +
                "database={Database}",
                partition.PartitionId,
                partition.DatabaseName);
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
                    /*
                     * Connection may already be broken.
                     */
                }

                await connection.DisposeAsync();
            }

            if (connectionSlotAcquired)
            {
                connectionLease?.Dispose();

                _logger.LogDebug(
                    "[RECONCILIATION_CONNECTION_SLOT_RELEASED] " +
                    "partition={PartitionId} " +
                    "database={Database}",
                    partition.PartitionId,
                    partition.DatabaseName);
            }
        }
    }

    private static bool IsNewEvent(
        LogDt3Row row,
        IReadOnlyDictionary<
            ReconciliationCheckpointKey,
            long> checkpoints)
    {
        var key =
            new ReconciliationCheckpointKey(
                row.KodeToko,
                row.JenisFile);

        /*
         * No checkpoint:
         *
         *     process available events.
         *
         * Existing checkpoint:
         *
         *     only process events with a newer tgl_proses.
         */
        return !checkpoints.TryGetValue(
                   key,
                   out var checkpoint)
               || row.TglProses > checkpoint;
    }
}