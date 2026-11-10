using Microsoft.Extensions.Logging;
using StreamApi.Api.Models;
using StreamApi.Api.TargetApi;
using StreamApi.PostgreSQL;
using StreamApi.Processing.Batching;
using StreamApi.Processing.Database;
using StreamApi.Processing.Monitoring;
using StreamApi.Processing.SourceReading;
using StreamApi.Processing.StreamedData;
using StreamApi.Processing.TableResolution;
using StreamApi.Routing;

namespace StreamApi.Processing;

public interface IProcessingEventHandler
{
    Task<ProcessingEventResult> HandleAsync(
        ReconciliationEvent reconciliationEvent,
        CancellationToken cancellationToken);
}

public sealed class ProcessingEventHandler
    : IProcessingEventHandler
{
    private readonly ILogger<ProcessingEventHandler>
        _logger;

    private readonly ITableResolver
        _tableResolver;

    private readonly StreamedDataStore
        _streamedDataStore;

    private readonly FileMonitoringStore
        _fileMonitoringStore;

    private readonly IProcessingConnectionResolver
        _connectionResolver;

    private readonly IProcessingConnectionProvider
        _connectionProvider;

    private readonly ISourceReader
        _sourceReader;

    private readonly BatchSizeController
        _batchSizeController;

    private readonly ReconciliationCheckpointStore
        _checkpointStore;

    private readonly PostgreSqlProcessingEventStateStore
        _processingEventStateStore;

    private readonly TargetApiPayloadBuilder
        _targetApiPayloadBuilder;

    private readonly ITargetApiClient
        _targetApiClient;


    public ProcessingEventHandler(
        ILogger<ProcessingEventHandler> logger,
        ITableResolver tableResolver,
        IProcessingConnectionResolver connectionResolver,
        IProcessingConnectionProvider connectionProvider,
        ISourceReader sourceReader,
        BatchSizeController batchSizeController,
        ReconciliationCheckpointStore checkpointStore,
        PostgreSqlProcessingEventStateStore processingEventStateStore,
        StreamedDataStore streamedDataStore,
        FileMonitoringStore fileMonitoringStore,
        TargetApiPayloadBuilder targetApiPayloadBuilder,
        ITargetApiClient targetApiClient)
    {
        _logger = logger;

        _tableResolver =
            tableResolver;

        _connectionResolver =
            connectionResolver;

        _connectionProvider =
            connectionProvider;

        _sourceReader =
            sourceReader;

        _batchSizeController =
            batchSizeController;

        _checkpointStore =
            checkpointStore;

        _processingEventStateStore =
            processingEventStateStore;

        _streamedDataStore =
            streamedDataStore;

        _fileMonitoringStore =
            fileMonitoringStore;

        _targetApiPayloadBuilder =
            targetApiPayloadBuilder;

        _targetApiClient =
            targetApiClient;
    }


    public async Task<ProcessingEventResult> HandleAsync(
        ReconciliationEvent reconciliationEvent,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "[PROCESSING_HANDLER_START] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglDt3={TglDt3} jumlahRecord={JumlahRecord} tglProses={TglProses}",
            reconciliationEvent.PartitionId,
            reconciliationEvent.DatabaseName,
            reconciliationEvent.KodeToko,
            reconciliationEvent.JenisFile,
            reconciliationEvent.TglDt3,
            reconciliationEvent.JumlahRecord,
            reconciliationEvent.TglProses);


        /*
         * ------------------------------------------------------------
         * 1. Resolve table configuration
         * ------------------------------------------------------------
         */

        var resolution =
            _tableResolver.Resolve(
                reconciliationEvent);

        if (!resolution.IsResolved)
        {
            _logger.LogWarning(
                "[TABLE_RESOLUTION_FAILED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} reason={Reason}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                resolution.Reason);

            return ProcessingEventResult.ConfigurationError(
                resolution.Reason ??
                "Unable to resolve table configuration.");
        }


        /*
         * ------------------------------------------------------------
         * 2. Build processing scope
         * ------------------------------------------------------------
         */

        var processingScope =
            new ProcessingScope(
                PartitionId:
                    reconciliationEvent.PartitionId,

                DatabaseName:
                    reconciliationEvent.DatabaseName,

                KodeToko:
                    reconciliationEvent.KodeToko,

                JenisFile:
                    reconciliationEvent.JenisFile,

                Schema:
                    resolution.Schema!,

                SourceTableName:
                    resolution.SourceTableName!,

                TargetTableName:
                    resolution.TargetTableName!,

                UseKodeTokoMapping:
                    resolution.UseKodeTokoMapping);


        /*
         * ------------------------------------------------------------
         * 3. Load durable event batch size
         *
         * The first attempt stores the current adaptive batch size.
         *
         * Every retry and every process restart reuses the same
         * persisted batch size for this event.
         * ------------------------------------------------------------
         */

        var eventBatchSize =
            await _processingEventStateStore
                .GetOrCreateBatchSizeAsync(
                    reconciliationEvent.KodeToko,
                    reconciliationEvent.JenisFile,
                    reconciliationEvent.TglProses,
                    _batchSizeController.CurrentBatchSize,
                    cancellationToken);


        _logger.LogInformation(
            "[PROCESSING_EVENT_BATCH_SIZE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} batchSize={BatchSize}",
            reconciliationEvent.PartitionId,
            reconciliationEvent.DatabaseName,
            reconciliationEvent.KodeToko,
            reconciliationEvent.JenisFile,
            reconciliationEvent.TglProses,
            eventBatchSize);


        _logger.LogInformation(
            "[PROCESSING_SCOPE_CREATED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} sourceTable={SourceTable} targetTable={TargetTable} kodeTokoMapping={KodeTokoMapping}",
            processingScope.PartitionId,
            processingScope.DatabaseName,
            processingScope.KodeToko,
            processingScope.JenisFile,
            processingScope.SourceTableName,
            processingScope.TargetTableName,
            processingScope.UseKodeTokoMapping);


        _logger.LogInformation(
            "[TABLE_RESOLVED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} schema={Schema} sourceTable={SourceTable} targetTable={TargetTable} kodeTokoMapping={KodeTokoMapping}",
            reconciliationEvent.PartitionId,
            reconciliationEvent.DatabaseName,
            reconciliationEvent.KodeToko,
            reconciliationEvent.JenisFile,
            resolution.Schema,
            resolution.SourceTableName,
            resolution.TargetTableName,
            resolution.UseKodeTokoMapping);


        /*
         * ------------------------------------------------------------
         * 4. Start file monitoring snapshot
         *
         * TargetTableName is the API/declared table name.
         *
         * Example:
         *
         *     source = pfe_pbe_T18K
         *     target = pfepbe
         *
         * Therefore the backup path is:
         *
         *     Backup_API/pfepbe/T18K/{tglProses}
         *
         * and NOT:
         *
         *     Backup_API/pfe_pbe_T18K/T18K/{tglProses}
         *
         * Starting the snapshot removes any previous snapshot for
         * the same event. This makes retries deterministic.
         * ------------------------------------------------------------
         */

        _fileMonitoringStore.StartSnapshot(
            processingScope.TargetTableName,
            processingScope.KodeToko,
            reconciliationEvent.TglProses);


        /*
         * ------------------------------------------------------------
         * 5. Resolve source connection
         * ------------------------------------------------------------
         */

        var connectionContext =
            _connectionResolver.Resolve(
                reconciliationEvent);


        _logger.LogInformation(
            "[PROCESSING_SOURCE_RESOLVED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} sourceDatabase={SourceDatabase}",
            reconciliationEvent.PartitionId,
            reconciliationEvent.DatabaseName,
            reconciliationEvent.KodeToko,
            reconciliationEvent.JenisFile,
            connectionContext.SourceDatabaseName);


        /*
         * ------------------------------------------------------------
         * 6. Create source read request
         *
         * eventBatchSize is immutable for this event because it comes
         * from PostgreSqlProcessingEventStateStore.
         * ------------------------------------------------------------
         */

        var sourceReadRequest =
            new SourceReadRequest(
                Event:
                    reconciliationEvent,

                Scope:
                    processingScope,

                Connection:
                    connectionContext,

                BatchSize:
                    eventBatchSize);


        long totalRowsStreamed = 0;

        long totalBatchesStreamed = 0;


        /*
         * ------------------------------------------------------------
         * 7. Open source connection and stream source batches
         * ------------------------------------------------------------
         */

        try
        {
            await using var connectionLease =
                await _connectionProvider.OpenAsync(
                    connectionContext.SourceDatabaseName,
                    cancellationToken);


            _logger.LogDebug(
                "[PROCESSING_SOURCE_CONNECTION_OPENED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile}",
                reconciliationEvent.PartitionId,
                connectionLease.Connection.Database,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile);


            await foreach (
                var batch in _sourceReader.ReadBatchesAsync(
                    sourceReadRequest,
                    connectionLease.Connection,
                    cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();


                _logger.LogInformation(
                    "[PROCESSING_BATCH_START] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} batch={BatchNumber} rows={Rows}",
                    processingScope.PartitionId,
                    connectionLease.Connection.Database,
                    processingScope.KodeToko,
                    processingScope.JenisFile,
                    batch.BatchNumber,
                    batch.RowCount);


                /*
                 * ----------------------------------------------------
                 * Store the already-streamed rows in RAM.
                 *
                 * TargetTableName is the API/declared table name.
                 *
                 * Example:
                 *
                 *     source = pfe_pbe_T18K
                 *     target = pfepbe
                 *
                 * Therefore:
                 *
                 *     pfepbe / T18K / tglProses
                 * ----------------------------------------------------
                 */

                _streamedDataStore.StoreBatch(
                    processingScope.TargetTableName,
                    processingScope.KodeToko,
                    processingScope.JenisFile,
                    reconciliationEvent.TglProses,
                    reconciliationEvent.TglDt3,
                    batch.BatchNumber,
                    batch.Rows);


                /*
                 * ----------------------------------------------------
                 * Persist the SAME already-streamed rows to the file
                 * monitoring backup.
                 *
                 * No second SELECT is performed.
                 *
                 * The data comes directly from batch.Rows.
                 * ----------------------------------------------------
                 */

                await _fileMonitoringStore.StoreBatchAsync(
                    processingScope.TargetTableName,
                    processingScope.KodeToko,
                    processingScope.JenisFile,
                    reconciliationEvent.TglProses,
                    reconciliationEvent.TglDt3,
                    batch.BatchNumber,
                    batch.Rows,
                    cancellationToken);


                totalRowsStreamed +=
                    batch.RowCount;

                totalBatchesStreamed++;


                _logger.LogInformation(
                    "[PROCESSING_BATCH_COMPLETE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} batch={BatchNumber} rows={Rows}",
                    processingScope.PartitionId,
                    connectionLease.Connection.Database,
                    processingScope.KodeToko,
                    processingScope.JenisFile,
                    batch.BatchNumber,
                    batch.RowCount);


                /*
                 * Successful batch allows the adaptive controller
                 * to increase the GLOBAL batch size.
                 *
                 * This does NOT modify eventBatchSize.
                 */

                _batchSizeController.Increase();
            }
        }
        catch (SourceTableNotFoundException ex)
        {
            _logger.LogWarning(
                "[SOURCE_TABLE_NOT_FOUND] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} schema={Schema} table={Table} reason={Reason}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                ex.Schema,
                ex.TableName,
                ex.Message);


            /*
             * The source could not be processed.
             *
             * Remove both the partial in-memory snapshot and the
             * partial file monitoring snapshot.
             *
             * This prevents MonitoringToko from exposing incomplete
             * data.
             */

            _streamedDataStore.TryRemove(
                processingScope.TargetTableName,
                processingScope.KodeToko,
                reconciliationEvent.TglProses,
                out _);


            _fileMonitoringStore.DeleteSnapshot(
                processingScope.TargetTableName,
                processingScope.KodeToko,
                reconciliationEvent.TglProses);


            return ProcessingEventResult.ConfigurationError(
                ex.Message);
        }
        catch
        {
            /*
             * Remove partial streamed data before allowing the worker
             * to retry the event.
             */

            _streamedDataStore.TryRemove(
                processingScope.TargetTableName,
                processingScope.KodeToko,
                reconciliationEvent.TglProses,
                out _);


            /*
             * Remove the partial file snapshot as well.
             *
             * The next retry will start with a clean snapshot.
             */

            _fileMonitoringStore.DeleteSnapshot(
                processingScope.TargetTableName,
                processingScope.KodeToko,
                reconciliationEvent.TglProses);


            throw;
        }


        /*
         * ------------------------------------------------------------
         * 8. Source processing complete
         * ------------------------------------------------------------
         */

        _logger.LogInformation(
            "[PROCESSING_SOURCE_COMPLETE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} batches={Batches} rows={Rows}",
            processingScope.PartitionId,
            reconciliationEvent.DatabaseName,
            processingScope.KodeToko,
            processingScope.JenisFile,
            totalBatchesStreamed,
            totalRowsStreamed);


        /*
         * ------------------------------------------------------------
         * 9. Empty source snapshot
         *
         * There is no Target API POST for an empty source.
         *
         * We still create an empty StreamedDataSnapshot so that a
         * successfully processed event with zero source rows can be
         * represented by MonitoringToko.
         *
         * The file backup also receives a completed.json marker.
         * ------------------------------------------------------------
         */

        if (totalBatchesStreamed == 0)
        {
            _streamedDataStore.GetOrCreate(
                processingScope.TargetTableName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                reconciliationEvent.TglProses,
                reconciliationEvent.TglDt3);


            _logger.LogInformation(
                "[PROCESSING_EMPTY_SNAPSHOT_COMPLETE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} expectedRows={ExpectedRows} rows=0",
                processingScope.PartitionId,
                reconciliationEvent.DatabaseName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                reconciliationEvent.TglProses,
                reconciliationEvent.JumlahRecord);


            try
            {
                await _fileMonitoringStore
                    .CompleteEmptySnapshotAsync(
                        processingScope.TargetTableName,
                        processingScope.KodeToko,
                        reconciliationEvent.TglProses,
                        processingScope.JenisFile,
                        reconciliationEvent.TglDt3,
                        cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[MONITORING_FILE_BACKUP_COMPLETE_FAILED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    processingScope.KodeToko,
                    processingScope.JenisFile,
                    reconciliationEvent.TglProses);


                _streamedDataStore.TryRemove(
                    processingScope.TargetTableName,
                    processingScope.KodeToko,
                    reconciliationEvent.TglProses,
                    out _);


                _fileMonitoringStore.DeleteSnapshot(
                    processingScope.TargetTableName,
                    processingScope.KodeToko,
                    reconciliationEvent.TglProses);


                return ProcessingEventResult.RetryableFailure(
                    $"Unable to complete monitoring file snapshot: {ex.Message}");
            }


            _batchSizeController.Increase();
        }


        /*
         * ------------------------------------------------------------
         * 10. Final row-count validation
         *
         * NORMAL DATA:
         *
         *     batches > 0
         *     streamed rows must equal expected rows.
         *
         * EMPTY DATA:
         *
         *     batches == 0
         *     empty snapshot has been created successfully.
         *
         * We do not reject an empty historical source snapshot merely
         * because the original event expected a non-zero count.
         * ------------------------------------------------------------
         */

        if (totalBatchesStreamed > 0 &&
            totalRowsStreamed !=
                reconciliationEvent.JumlahRecord)
        {
            _logger.LogError(
                "[PROCESSING_ROW_COUNT_MISMATCH] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} expectedRows={ExpectedRows} streamedRows={StreamedRows}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                reconciliationEvent.JumlahRecord,
                totalRowsStreamed);


            /*
             * The snapshot is incomplete/invalid.
             */

            _streamedDataStore.TryRemove(
                processingScope.TargetTableName,
                processingScope.KodeToko,
                reconciliationEvent.TglProses,
                out _);


            _fileMonitoringStore.DeleteSnapshot(
                processingScope.TargetTableName,
                processingScope.KodeToko,
                reconciliationEvent.TglProses);


            return ProcessingEventResult.RetryableFailure(
                $"Row count mismatch. Expected {reconciliationEvent.JumlahRecord}, streamed {totalRowsStreamed}.");
        }


        /*
         * ------------------------------------------------------------
         * 11. Complete non-empty file monitoring snapshot
         *
         * IMPORTANT:
         *
         * completed.json is created ONLY after every batch has been
         * successfully written and the row count has been validated.
         *
         * MonitoringToko can therefore safely treat the presence of
         * completed.json as "complete".
         * ------------------------------------------------------------
         */

        if (totalBatchesStreamed > 0)
        {
            try
            {
                await _fileMonitoringStore
                    .CompleteSnapshotAsync(
                        processingScope.TargetTableName,
                        processingScope.KodeToko,
                        reconciliationEvent.TglProses,
                        processingScope.JenisFile,
                        reconciliationEvent.TglDt3,
                        reconciliationEvent.JumlahRecord,
                        totalRowsStreamed,
                        totalBatchesStreamed,
                        cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[MONITORING_FILE_BACKUP_COMPLETE_FAILED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    processingScope.KodeToko,
                    processingScope.JenisFile,
                    reconciliationEvent.TglProses);


                _streamedDataStore.TryRemove(
                    processingScope.TargetTableName,
                    processingScope.KodeToko,
                    reconciliationEvent.TglProses,
                    out _);


                _fileMonitoringStore.DeleteSnapshot(
                    processingScope.TargetTableName,
                    processingScope.KodeToko,
                    reconciliationEvent.TglProses);


                return ProcessingEventResult.RetryableFailure(
                    $"Unable to complete monitoring file snapshot: {ex.Message}");
            }
        }


        /*
         * ------------------------------------------------------------
         * 12. POST metadata to Target API
         *
         * The Target API contract currently receives:
         *
         *     KodeToko
         *     Table
         *     DataServer
         *     TanggalProses
         *     TanggalDT3
         *
         * Table comes from tables.json through
         * processingScope.TargetTableName.
         *
         * DataServer uses the event's expected record count.
         *
         * There is intentionally NO /login or JWT handling here yet.
         * ------------------------------------------------------------
         */

        try
        {
            var targetApiPayload =
                _targetApiPayloadBuilder.Build(
                    reconciliationEvent,
                    processingScope.TargetTableName,
                    reconciliationEvent.JumlahRecord);

            _logger.LogInformation(
                "[TARGET_API_POST_PREPARE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} targetTable={TargetTable} dataServer={DataServer} tglProses={TglProses} tglDt3={TglDt3}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                processingScope.TargetTableName,
                reconciliationEvent.JumlahRecord,
                reconciliationEvent.TglProses,
                reconciliationEvent.TglDt3);


            var targetApiResult =
                await _targetApiClient.PostAsync(
                    targetApiPayload,
                    cancellationToken);


            if (!targetApiResult.IsSuccessful)
            {
                _logger.LogWarning(
                    "[TARGET_API_POST_FAILED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} targetTable={TargetTable} statusCode={StatusCode} response={Response}",
                    reconciliationEvent.PartitionId,
                    reconciliationEvent.DatabaseName,
                    processingScope.KodeToko,
                    processingScope.JenisFile,
                    processingScope.TargetTableName,
                    targetApiResult.StatusCode,
                    targetApiResult.ResponseBody);


                return ProcessingEventResult.RetryableFailure(
                    $"Target API POST failed with status code {targetApiResult.StatusCode}.");
            }


            _logger.LogInformation(
                "[TARGET_API_POST_SUCCESS] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} targetTable={TargetTable} dataServer={DataServer} statusCode={StatusCode}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                processingScope.TargetTableName,
                reconciliationEvent.JumlahRecord,
                targetApiResult.StatusCode);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[TARGET_API_POST_EXCEPTION] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} targetTable={TargetTable}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                processingScope.TargetTableName);


            return ProcessingEventResult.RetryableFailure(
                $"Unable to POST to Target API: {ex.Message}");
        }


        /*
         * ------------------------------------------------------------
         * 13. Advance durable checkpoint
         *
         * Checkpoint is advanced only after:
         *
         *     source stream completed
         *     rows were validated
         *     monitoring snapshot was completed
         *     Target API POST succeeded
         * ------------------------------------------------------------
         */

        try
        {
            await _checkpointStore.AdvanceAsync(
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[CHECKPOINT_PERSIST_FAILED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                reconciliationEvent.TglProses);


            /*
             * The monitoring snapshot itself is valid, and the
             * Target API POST succeeded, but the event has not been
             * durably checkpointed.
             *
             * Keep the snapshot so MonitoringToko can still use the
             * successfully streamed data.
             *
             * A retry can cause the Target API POST to happen again.
             * Idempotency can be added later to the Target API contract.
             */

            return ProcessingEventResult.RetryableFailure(
                $"Unable to persist checkpoint: {ex.Message}");
        }


        _logger.LogInformation(
            "[CHECKPOINT_ADVANCE] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
            processingScope.PartitionId,
            reconciliationEvent.DatabaseName,
            processingScope.KodeToko,
            processingScope.JenisFile,
            reconciliationEvent.TglProses);


        /*
         * ------------------------------------------------------------
         * 14. Delete durable event execution state
         * ------------------------------------------------------------
         */

        try
        {
            await _processingEventStateStore.DeleteAsync(
                reconciliationEvent.KodeToko,
                reconciliationEvent.JenisFile,
                reconciliationEvent.TglProses,
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "[PROCESSING_EVENT_STATE_CLEANUP_FAILED] partition={PartitionId} database={Database} toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
                reconciliationEvent.PartitionId,
                reconciliationEvent.DatabaseName,
                processingScope.KodeToko,
                processingScope.JenisFile,
                reconciliationEvent.TglProses);
        }


        /*
         * ------------------------------------------------------------
         * 15. Processing success
         * ------------------------------------------------------------
         */

        return ProcessingEventResult.Success(
            totalRowsStreamed,
            totalBatchesStreamed);
    }
}