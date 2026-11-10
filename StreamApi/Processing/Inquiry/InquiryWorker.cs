using StreamApi.PostgreSQL;
using System.Threading.Channels;

namespace StreamApi.Processing.Inquiry;

public sealed class InquiryWorker
    : BackgroundService
{
    private readonly Channel<LogDt3Row> _queue;
    private readonly InquiryService _inquiryService;
    private readonly ILogger<InquiryWorker> _logger;

    private readonly SemaphoreSlim _persistenceSignal =
        new(0, 1);

    private readonly TimeSpan _persistenceDelay =
        TimeSpan.FromMilliseconds(500);

    public InquiryWorker(
        InquiryService inquiryService,
        ILogger<InquiryWorker> logger)
    {
        _inquiryService =
            inquiryService;

        _logger =
            logger;

        /*
         * IMPORTANT:
         *
         * Inquiry must not lose notifications.
         *
         * The previous bounded channel used:
         *
         *     DropWrite
         *
         * which could silently discard valid log_dt3_proses
         * notifications.
         *
         * The queue is now unbounded.
         *
         * PostgreSQL notifications only place a small LogDt3Row
         * object into this queue. Database/file work remains in
         * the background worker.
         */
        _queue =
            Channel.CreateUnbounded<LogDt3Row>(
                new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false
                });
    }

    public int Pending =>
        _queue.Reader.CanCount
            ? _queue.Reader.Count
            : -1;

    public bool TryEnqueue(
        LogDt3Row row)
    {
        ArgumentNullException.ThrowIfNull(
            row);

        var written =
            _queue.Writer.TryWrite(
                row);

        if (!written)
        {
            _logger.LogError(
                "[INQUIRY_QUEUE_WRITE_FAILED] " +
                "toko={KodeToko} " +
                "jenisFile={JenisFile} " +
                "tglProses={TglProses}",
                row.KodeToko,
                row.JenisFile,
                row.TglProses);
        }

        return written;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        /*
         * Do not process notification events until the
         * retained database bootstrap has completed.
         *
         * This prevents a notification arriving during bootstrap
         * from being inserted into RAM and then overwritten by
         * InquiryStore.Replace().
         */
        try
        {
            await _inquiryService
                .WaitUntilInitializedAsync(
                    stoppingToken);

            _logger.LogInformation(
                "[INQUIRY_WORKER_STARTED] " +
                "rows={RowCount} " +
                "pending={Pending}",
                _inquiryService.RowCount,
                Pending);

            var processingTask =
                ProcessQueueAsync(
                    stoppingToken);

            var persistenceTask =
                PersistenceLoopAsync(
                    stoppingToken);

            await Task.WhenAll(
                processingTask,
                persistenceTask);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[INQUIRY_WORKER_FAILED]");
        }
        finally
        {
            try
            {
                await _inquiryService
                    .PersistCurrentSnapshotAsync(
                        CancellationToken.None);

                _logger.LogInformation(
                    "[INQUIRY_FINAL_SNAPSHOT_PERSISTED] " +
                    "rows={RowCount}",
                    _inquiryService.RowCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[INQUIRY_FINAL_SNAPSHOT_PERSIST_FAILED]");
            }

            _logger.LogInformation(
                "[INQUIRY_WORKER_STOPPED]");
        }
    }

    public override async Task StopAsync(
        CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();

        if (_persistenceSignal.CurrentCount == 0)
        {
            _persistenceSignal.Release();
        }

        await base.StopAsync(
            cancellationToken);
    }

    private async Task ProcessQueueAsync(
        CancellationToken cancellationToken)
    {
        await foreach (
            var row in
            _queue.Reader.ReadAllAsync(
                cancellationToken))
        {
            try
            {
                var accepted =
                    _inquiryService
                        .ProcessNotification(
                            row);

                if (!accepted)
                {
                    continue;
                }

                /*
                 * Coalesce persistence requests.
                 *
                 * Multiple notifications arriving during the
                 * debounce period result in one file write.
                 */
                if (_persistenceSignal.CurrentCount == 0)
                {
                    _persistenceSignal.Release();
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[INQUIRY_WORKER_ITEM_FAILED] " +
                    "toko={KodeToko} " +
                    "jenisFile={JenisFile} " +
                    "tglProses={TglProses}",
                    row.KodeToko,
                    row.JenisFile,
                    row.TglProses);
            }
        }
    }

    private async Task PersistenceLoopAsync(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _persistenceSignal.WaitAsync(
                    cancellationToken);

                /*
                 * Debounce persistence.
                 *
                 * Notifications arriving during this period
                 * are included in the same snapshot.
                 */
                await Task.Delay(
                    _persistenceDelay,
                    cancellationToken);

                await _inquiryService
                    .PersistCurrentSnapshotAsync(
                        cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[INQUIRY_PERSISTENCE_FAILED]");
            }
        }
    }
}