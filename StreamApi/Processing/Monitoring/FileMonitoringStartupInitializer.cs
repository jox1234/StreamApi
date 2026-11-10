using Microsoft.Extensions.Hosting;

namespace StreamApi.Processing.Monitoring;

public sealed class FileMonitoringStartupInitializer
    : IHostedService
{
    private readonly FileMonitoringStore
        _fileMonitoringStore;

    private readonly ILogger<
        FileMonitoringStartupInitializer>
        _logger;

    public FileMonitoringStartupInitializer(
        FileMonitoringStore fileMonitoringStore,
        ILogger<FileMonitoringStartupInitializer> logger)
    {
        _fileMonitoringStore =
            fileMonitoringStore;

        _logger =
            logger;
    }

    public async Task StartAsync(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_INITIALIZING]");

        _fileMonitoringStore
            .EnsureBackupDirectory();

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_INITIALIZED] " +
            "rootPath={RootPath} " +
            "backupPath={BackupPath} " +
            "retentionDays={RetentionDays}",
            _fileMonitoringStore.RootPath,
            _fileMonitoringStore.BackupRootPath,
            _fileMonitoringStore.RetentionDays);

        var deletedSnapshots =
            await _fileMonitoringStore
                .CleanupExpiredSnapshotsAsync(
                    cancellationToken);

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_RETENTION_INITIALIZED] " +
            "retentionDays={RetentionDays} " +
            "deletedSnapshots={DeletedSnapshots}",
            _fileMonitoringStore.RetentionDays,
            deletedSnapshots);
    }

    public Task StopAsync(
        CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}