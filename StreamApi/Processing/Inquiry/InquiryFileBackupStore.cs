using System.Text.Json;
using StreamApi.PostgreSQL;

namespace StreamApi.Processing.Inquiry;

public sealed class InquiryFileBackupStore
{
    private readonly object _sync = new();

    private readonly string _rootPath;

    private readonly JsonSerializerOptions _jsonOptions =
        new()
        {
            WriteIndented = false
        };

    private readonly ILogger<InquiryFileBackupStore>
        _logger;

    public InquiryFileBackupStore(
        IConfiguration configuration,
        ILogger<InquiryFileBackupStore> logger)
    {
        _logger = logger;

        var rootPath =
            configuration["MonitoringBackup:RootPath"];

        if (string.IsNullOrWhiteSpace(rootPath))
        {
            throw new InvalidOperationException(
                "MonitoringBackup:RootPath is not configured.");
        }

        _rootPath =
            Path.GetFullPath(rootPath);

        Directory.CreateDirectory(
            GetInquiryDirectory());

        _logger.LogInformation(
            "[INQUIRY_FILE_BACKUP_DIRECTORY_READY] " +
            "directory={Directory}",
            GetInquiryDirectory());
    }

    public string InquiryDirectory =>
        GetInquiryDirectory();

    public async Task WriteAsync(
        DateOnly processingDate,
        IReadOnlyList<LogDt3Row> rows,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(rows);

        cancellationToken.ThrowIfCancellationRequested();

        var directory =
            GetInquiryDirectory();

        Directory.CreateDirectory(
            directory);

        var filePath =
            GetFilePath(
                processingDate);

        var temporaryFilePath =
            filePath + ".tmp";

        var backup =
            new InquiryFileBackup(
                processingDate,
                DateTime.UtcNow,
                rows);

        lock (_sync)
        {
            Directory.CreateDirectory(
                directory);
        }

        await using (
            var stream =
                new FileStream(
                    temporaryFilePath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 64 * 1024,
                    useAsync: true))
        {
            await JsonSerializer.SerializeAsync(
                stream,
                backup,
                _jsonOptions,
                cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();

        lock (_sync)
        {
            File.Move(
                temporaryFilePath,
                filePath,
                overwrite: true);
        }

        _logger.LogInformation(
            "[INQUIRY_FILE_BACKUP_SAVED] " +
            "date={ProcessingDate} " +
            "file={FilePath} " +
            "rows={RowCount}",
            processingDate,
            filePath,
            rows.Count);
    }

    public async Task<
        IReadOnlyList<LogDt3Row>>
        ReadAsync(
            DateOnly processingDate,
            CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var filePath =
            GetFilePath(
                processingDate);

        if (!File.Exists(filePath))
        {
            _logger.LogInformation(
                "[INQUIRY_FILE_BACKUP_NOT_FOUND] " +
                "date={ProcessingDate} " +
                "file={FilePath}",
                processingDate,
                filePath);

            return Array.Empty<LogDt3Row>();
        }

        await using var stream =
            new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 64 * 1024,
                useAsync: true);

        var backup =
            await JsonSerializer.DeserializeAsync<
                InquiryFileBackup>(
                stream,
                _jsonOptions,
                cancellationToken);

        if (backup is null)
        {
            _logger.LogWarning(
                "[INQUIRY_FILE_BACKUP_EMPTY] " +
                "date={ProcessingDate} " +
                "file={FilePath}",
                processingDate,
                filePath);

            return Array.Empty<LogDt3Row>();
        }

        _logger.LogInformation(
            "[INQUIRY_FILE_BACKUP_READ] " +
            "date={ProcessingDate} " +
            "file={FilePath} " +
            "rows={RowCount}",
            processingDate,
            filePath,
            backup.Data.Count);

        return backup.Data;
    }

    public async Task<
        IReadOnlyList<LogDt3Row>>
        ReadLatestAsync(
            CancellationToken cancellationToken)
    {
        var today =
            DateOnly.FromDateTime(
                DateTime.Now);

        return await ReadAsync(
            today,
            cancellationToken);
    }

    public Task<int> CleanupExpiredAsync(
        int retentionDays,
        CancellationToken cancellationToken)
    {
        if (retentionDays < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(retentionDays),
                retentionDays,
                "RetentionDays cannot be negative.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        var directory =
            GetInquiryDirectory();

        if (!Directory.Exists(directory))
        {
            return Task.FromResult(0);
        }

        var today =
            DateOnly.FromDateTime(
                DateTime.Now);

        var cutoffDate =
            today.AddDays(-retentionDays);

        var deletedFiles = 0;

        _logger.LogInformation(
            "[INQUIRY_FILE_BACKUP_RETENTION_START] " +
            "retentionDays={RetentionDays} " +
            "today={Today} " +
            "cutoffDate={CutoffDate}",
            retentionDays,
            today,
            cutoffDate);

        foreach (var filePath in
                 Directory.EnumerateFiles(
                     directory,
                     "*.json",
                     SearchOption.TopDirectoryOnly))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileName =
                Path.GetFileNameWithoutExtension(
                    filePath);

            if (!DateOnly.TryParseExact(
                    fileName,
                    "yyyyMMdd",
                    out var fileDate))
            {
                continue;
            }

            if (fileDate >= cutoffDate)
            {
                continue;
            }

            try
            {
                File.Delete(filePath);

                deletedFiles++;

                _logger.LogInformation(
                    "[INQUIRY_FILE_BACKUP_RETENTION_DELETED] " +
                    "file={FilePath} " +
                    "fileDate={FileDate} " +
                    "cutoffDate={CutoffDate}",
                    filePath,
                    fileDate,
                    cutoffDate);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "[INQUIRY_FILE_BACKUP_RETENTION_DELETE_FAILED] " +
                    "file={FilePath} " +
                    "fileDate={FileDate}",
                    filePath,
                    fileDate);
            }
        }

        _logger.LogInformation(
            "[INQUIRY_FILE_BACKUP_RETENTION_COMPLETE] " +
            "retentionDays={RetentionDays} " +
            "today={Today} " +
            "cutoffDate={CutoffDate} " +
            "deletedFiles={DeletedFiles}",
            retentionDays,
            today,
            cutoffDate,
            deletedFiles);

        return Task.FromResult(
            deletedFiles);
    }

    private string GetInquiryDirectory()
    {
        return Path.Combine(
            _rootPath,
            "Backup_API",
            "inquiry");
    }

    private string GetFilePath(
        DateOnly processingDate)
    {
        return Path.Combine(
            GetInquiryDirectory(),
            $"{processingDate:yyyyMMdd}.json");
    }
}

public sealed record InquiryFileBackup(
    DateOnly ProcessingDate,
    DateTime SavedAtUtc,
    IReadOnlyList<LogDt3Row> Data);