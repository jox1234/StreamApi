using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;

namespace StreamApi.Processing.Monitoring;

public sealed class FileMonitoringStore
{
    private const string BackupDirectoryName = "Backup_API";

    private const string CompletedFileName =
        "completed.json";

    private readonly string _rootPath;

    private readonly int _retentionDays;

    private readonly ILogger<FileMonitoringStore>
        _logger;

    private readonly JsonSerializerOptions
        _jsonOptions;

    public FileMonitoringStore(
        IOptions<MonitoringBackupOptions> options,
        ILogger<FileMonitoringStore> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        if (string.IsNullOrWhiteSpace(
                options.Value.RootPath))
        {
            throw new InvalidOperationException(
                "MonitoringBackup:RootPath is not configured.");
        }

        if (options.Value.RetentionDays < 0)
        {
            throw new InvalidOperationException(
                "MonitoringBackup:RetentionDays cannot be negative.");
        }

        _rootPath =
            Path.GetFullPath(
                options.Value.RootPath);

        _retentionDays =
            options.Value.RetentionDays;

        _logger = logger;

        _jsonOptions =
            new JsonSerializerOptions
            {
                WriteIndented = false,
                PropertyNamingPolicy =
                    JsonNamingPolicy.CamelCase,
                DefaultIgnoreCondition =
                    JsonIgnoreCondition.Never
            };

        // ----------------------------------------------------
        // Initialize backup directory when StreamApi starts
        // ----------------------------------------------------

        EnsureBackupDirectory();
    }

    /// <summary>
    /// Configured root directory.
    ///
    /// Windows:
    /// D:\FtpGrab
    ///
    /// Linux:
    /// /root/FtpGrab
    /// </summary>
    public string RootPath =>
        _rootPath;

    /// <summary>
    /// Configured retention period in days.
    ///
    /// RetentionDays = 1 means:
    ///
    /// - today is kept
    /// - yesterday is kept
    /// - the day before yesterday and older are deleted
    /// </summary>
    public int RetentionDays =>
        _retentionDays;

    /// <summary>
    /// Returns:
    ///
    /// /root/FtpGrab/Backup_API
    ///
    /// or
    ///
    /// D:\FtpGrab\Backup_API
    /// </summary>
    public string BackupRootPath
    {
        get
        {
            return Path.Combine(
                _rootPath,
                BackupDirectoryName);
        }
    }

    /// <summary>
    /// Ensures the root backup directory exists.
    /// </summary>
    public void EnsureBackupDirectory()
    {
        Directory.CreateDirectory(
            BackupRootPath);

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_DIRECTORY_READY] path={Path}",
            BackupRootPath);
    }

    /// <summary>
    /// Removes completed monitoring snapshots whose
    /// tglProses date is older than the configured retention.
    ///
    /// The retention decision is based ONLY on the
    /// yyyyMMdd portion of the tglProses directory name.
    ///
    /// Example:
    ///
    /// tglProses:
    /// 202609181501129909
    ///
    /// Date:
    /// 20260918
    ///
    /// The complete tglProses directory is deleted.
    /// Individual batch files are never deleted separately.
    /// </summary>
    public Task<int> CleanupExpiredSnapshotsAsync(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        EnsureBackupDirectory();

        var today =
            DateOnly.FromDateTime(
                DateTime.Now);

        var cutoffDate =
            today.AddDays(
                -_retentionDays);

        var deletedSnapshots = 0;

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_RETENTION_START] " +
            "retentionDays={RetentionDays} " +
            "today={Today:yyyy-MM-dd} " +
            "cutoffDate={CutoffDate:yyyy-MM-dd}",
            _retentionDays,
            today,
            cutoffDate);

        try
        {
            var tableDirectories =
                Directory.EnumerateDirectories(
                    BackupRootPath,
                    "*",
                    SearchOption.TopDirectoryOnly);

            foreach (var tableDirectory in tableDirectories)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var tableName =
                    Path.GetFileName(
                        tableDirectory);

                if (string.IsNullOrWhiteSpace(
                        tableName))
                {
                    continue;
                }

                CleanupTableDirectory(
                    tableDirectory,
                    tableName,
                    cutoffDate,
                    cancellationToken,
                    ref deletedSnapshots);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "[MONITORING_FILE_BACKUP_RETENTION_FAILED] " +
                "retentionDays={RetentionDays} " +
                "cutoffDate={CutoffDate:yyyy-MM-dd}",
                _retentionDays,
                cutoffDate);
        }

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_RETENTION_COMPLETE] " +
            "retentionDays={RetentionDays} " +
            "today={Today:yyyy-MM-dd} " +
            "cutoffDate={CutoffDate:yyyy-MM-dd} " +
            "deletedSnapshots={DeletedSnapshots}",
            _retentionDays,
            today,
            cutoffDate,
            deletedSnapshots);

        return Task.FromResult(
            deletedSnapshots);
    }

    /// <summary>
    /// Returns the directory for one monitoring snapshot:
    ///
    /// /Backup_API/{tableName}/{kodeToko}/{tglProses}
    /// </summary>
    public string GetSnapshotDirectory(
        string tableName,
        string kodeToko,
        long tglProses)
    {
        ValidateSegment(
            tableName,
            nameof(tableName));

        ValidateSegment(
            kodeToko,
            nameof(kodeToko));

        var tglProsesDirectory =
            tglProses.ToString(
                CultureInfo.InvariantCulture);

        return Path.Combine(
            BackupRootPath,
            tableName,
            kodeToko,
            tglProsesDirectory);
    }

    /// <summary>
    /// Creates a new processing snapshot directory.
    ///
    /// Any previous snapshot for the same
    /// table/store/tglProses is removed first.
    ///
    /// This is important for deterministic retries.
    /// </summary>
    public void StartSnapshot(
        string tableName,
        string kodeToko,
        long tglProses)
    {
        var snapshotDirectory =
            GetSnapshotDirectory(
                tableName,
                kodeToko,
                tglProses);

        EnsureBackupDirectory();

        if (Directory.Exists(
                snapshotDirectory))
        {
            Directory.Delete(
                snapshotDirectory,
                recursive: true);

            _logger.LogInformation(
                "[MONITORING_FILE_BACKUP_OLD_SNAPSHOT_REMOVED] " +
                "table={TableName} toko={KodeToko} " +
                "tglProses={TglProses} path={Path}",
                tableName,
                kodeToko,
                tglProses,
                snapshotDirectory);
        }

        Directory.CreateDirectory(
            snapshotDirectory);

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_SNAPSHOT_STARTED] " +
            "table={TableName} toko={KodeToko} " +
            "tglProses={TglProses} path={Path}",
            tableName,
            kodeToko,
            tglProses,
            snapshotDirectory);
    }

    /// <summary>
    /// Stores one batch:
    ///
    /// batch-000001.json
    /// batch-000002.json
    /// etc.
    ///
    /// The file is first written to .tmp and then
    /// moved into place.
    /// </summary>
    public async Task StoreBatchAsync(
        string tableName,
        string kodeToko,
        string jenisFile,
        long tglProses,
        DateOnly tglDt3,
        long batchNumber,
        IReadOnlyList<
            Dictionary<string, JsonElement>> rows,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(rows);

        if (batchNumber <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchNumber),
                batchNumber,
                "Batch number must be greater than zero.");
        }

        var snapshotDirectory =
            GetSnapshotDirectory(
                tableName,
                kodeToko,
                tglProses);

        Directory.CreateDirectory(
            snapshotDirectory);

        var fileName =
            $"batch-{batchNumber:D6}.json";

        var filePath =
            Path.Combine(
                snapshotDirectory,
                fileName);

        var temporaryFilePath =
            filePath + ".tmp";

        var batch =
            new FileMonitoringBatch
            {
                TableName = tableName,
                KodeToko = kodeToko,
                JenisFile = jenisFile,
                TglProses = tglProses,
                TglDt3 = tglDt3,
                BatchNumber = batchNumber,
                RowCount = rows.Count,
                Data = rows
            };

        try
        {
            await using (
                var stream =
                    new FileStream(
                        temporaryFilePath,
                        FileMode.Create,
                        FileAccess.Write,
                        FileShare.None,
                        bufferSize: 64 * 1024,
                        options:
                            FileOptions.Asynchronous |
                            FileOptions.SequentialScan))
            {
                await JsonSerializer.SerializeAsync(
                    stream,
                    batch,
                    _jsonOptions,
                    cancellationToken);

                await stream.FlushAsync(
                    cancellationToken);
            }

            File.Move(
                temporaryFilePath,
                filePath,
                overwrite: true);

            _logger.LogInformation(
                "[MONITORING_FILE_BACKUP_BATCH_SAVED] " +
                "table={TableName} toko={KodeToko} " +
                "tglProses={TglProses} batch={BatchNumber} " +
                "rows={RowCount} path={Path}",
                tableName,
                kodeToko,
                tglProses,
                batchNumber,
                rows.Count,
                filePath);
        }
        catch
        {
            TryDeleteFile(
                temporaryFilePath);

            throw;
        }
    }

    /// <summary>
    /// Creates the completion marker.
    ///
    /// MonitoringToko must only expose a snapshot
    /// after this file exists.
    /// </summary>
    public async Task CompleteSnapshotAsync(
        string tableName,
        string kodeToko,
        long tglProses,
        string jenisFile,
        DateOnly tglDt3,
        long expectedRowCount,
        long rowCount,
        long batchCount,
        CancellationToken cancellationToken)
    {
        var snapshotDirectory =
            GetSnapshotDirectory(
                tableName,
                kodeToko,
                tglProses);

        Directory.CreateDirectory(
            snapshotDirectory);

        var completedFilePath =
            Path.Combine(
                snapshotDirectory,
                CompletedFileName);

        var temporaryFilePath =
            completedFilePath + ".tmp";

        var completion =
            new FileMonitoringCompletion
            {
                TableName = tableName,
                KodeToko = kodeToko,
                JenisFile = jenisFile,
                TglProses = tglProses,
                TglDt3 = tglDt3,
                ExpectedRowCount =
                    expectedRowCount,
                RowCount =
                    rowCount,
                BatchCount =
                    batchCount,
                CompletedAtUtc =
                    DateTimeOffset.UtcNow
            };

        try
        {
            await using (
                var stream =
                    new FileStream(
                        temporaryFilePath,
                        FileMode.Create,
                        FileAccess.Write,
                        FileShare.None,
                        bufferSize: 16 * 1024,
                        options:
                            FileOptions.Asynchronous |
                            FileOptions.SequentialScan))
            {
                await JsonSerializer.SerializeAsync(
                    stream,
                    completion,
                    _jsonOptions,
                    cancellationToken);

                await stream.FlushAsync(
                    cancellationToken);
            }

            File.Move(
                temporaryFilePath,
                completedFilePath,
                overwrite: true);

            _logger.LogInformation(
                "[MONITORING_FILE_BACKUP_COMPLETED] " +
                "table={TableName} toko={KodeToko} " +
                "tglProses={TglProses} rows={RowCount} " +
                "batches={BatchCount} path={Path}",
                tableName,
                kodeToko,
                tglProses,
                rowCount,
                batchCount,
                snapshotDirectory);
        }
        catch
        {
            TryDeleteFile(
                temporaryFilePath);

            throw;
        }
    }

    /// <summary>
    /// Marks an empty event as completed.
    /// </summary>
    public Task CompleteEmptySnapshotAsync(
        string tableName,
        string kodeToko,
        long tglProses,
        string jenisFile,
        DateOnly tglDt3,
        CancellationToken cancellationToken)
    {
        return CompleteSnapshotAsync(
            tableName,
            kodeToko,
            tglProses,
            jenisFile,
            tglDt3,
            expectedRowCount: 0,
            rowCount: 0,
            batchCount: 0,
            cancellationToken);
    }

    /// <summary>
    /// Determines whether a completed snapshot exists.
    /// </summary>
    public bool IsSnapshotCompleted(
        string tableName,
        string kodeToko,
        long tglProses)
    {
        var snapshotDirectory =
            GetSnapshotDirectory(
                tableName,
                kodeToko,
                tglProses);

        var completedFilePath =
            Path.Combine(
                snapshotDirectory,
                CompletedFileName);

        return File.Exists(
            completedFilePath);
    }

    /// <summary>
    /// Removes the complete snapshot directory.
    /// </summary>
    public void DeleteSnapshot(
        string tableName,
        string kodeToko,
        long tglProses)
    {
        var snapshotDirectory =
            GetSnapshotDirectory(
                tableName,
                kodeToko,
                tglProses);

        if (!Directory.Exists(
                snapshotDirectory))
        {
            return;
        }

        Directory.Delete(
            snapshotDirectory,
            recursive: true);

        _logger.LogInformation(
            "[MONITORING_FILE_BACKUP_SNAPSHOT_DELETED] " +
            "table={TableName} toko={KodeToko} " +
            "tglProses={TglProses} path={Path}",
            tableName,
            kodeToko,
            tglProses,
            snapshotDirectory);
    }

    /// <summary>
    /// Reads all batches from a completed snapshot.
    ///
    /// If the snapshot isn't completed, an empty result
    /// is returned.
    /// </summary>
    public async Task<
        IReadOnlyList<
            Dictionary<string, JsonElement>>>
        ReadCompletedSnapshotAsync(
            string tableName,
            string kodeToko,
            long tglProses,
            CancellationToken cancellationToken)
    {
        var snapshotDirectory =
            GetSnapshotDirectory(
                tableName,
                kodeToko,
                tglProses);

        var completedFilePath =
            Path.Combine(
                snapshotDirectory,
                CompletedFileName);

        if (!File.Exists(
                completedFilePath))
        {
            return [];
        }

        var batchFiles =
            Directory.GetFiles(
                    snapshotDirectory,
                    "batch-*.json",
                    SearchOption.TopDirectoryOnly)
                .OrderBy(
                    x => x,
                    StringComparer.Ordinal)
                .ToArray();

        var results =
            new List<
                Dictionary<string, JsonElement>>();

        foreach (var batchFile in batchFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using var stream =
                new FileStream(
                    batchFile,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize: 64 * 1024,
                    options:
                        FileOptions.Asynchronous |
                        FileOptions.SequentialScan);

            var batch =
                await JsonSerializer.DeserializeAsync<
                    FileMonitoringBatch>(
                    stream,
                    _jsonOptions,
                    cancellationToken);

            if (batch?.Data is null)
            {
                continue;
            }

            results.AddRange(
                batch.Data);
        }

        _logger.LogDebug(
            "[MONITORING_FILE_BACKUP_READ_COMPLETE] " +
            "table={TableName} toko={KodeToko} " +
            "tglProses={TglProses} batches={BatchCount} rows={RowCount}",
            tableName,
            kodeToko,
            tglProses,
            batchFiles.Length,
            results.Count);

        return results;
    }

    private void CleanupTableDirectory(
        string tableDirectory,
        string tableName,
        DateOnly cutoffDate,
        CancellationToken cancellationToken,
        ref int deletedSnapshots)
    {
        try
        {
            var tokoDirectories =
                Directory.EnumerateDirectories(
                    tableDirectory,
                    "*",
                    SearchOption.TopDirectoryOnly);

            foreach (var tokoDirectory in tokoDirectories)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var kodeToko =
                    Path.GetFileName(
                        tokoDirectory);

                if (string.IsNullOrWhiteSpace(
                        kodeToko))
                {
                    continue;
                }

                CleanupTokoDirectory(
                    tokoDirectory,
                    tableName,
                    kodeToko,
                    cutoffDate,
                    cancellationToken,
                    ref deletedSnapshots);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "[MONITORING_FILE_BACKUP_RETENTION_TABLE_FAILED] " +
                "table={TableName} path={Path}",
                tableName,
                tableDirectory);
        }
    }

    private void CleanupTokoDirectory(
        string tokoDirectory,
        string tableName,
        string kodeToko,
        DateOnly cutoffDate,
        CancellationToken cancellationToken,
        ref int deletedSnapshots)
    {
        try
        {
            var snapshotDirectories =
                Directory.EnumerateDirectories(
                    tokoDirectory,
                    "*",
                    SearchOption.TopDirectoryOnly);

            foreach (var snapshotDirectory in snapshotDirectories)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var directoryName =
                    Path.GetFileName(
                        snapshotDirectory);

                if (!TryGetTglProsesDate(
                        directoryName,
                        out var tglProses,
                        out var tglProsesDate))
                {
                    _logger.LogDebug(
                        "[MONITORING_FILE_BACKUP_RETENTION_SKIP] " +
                        "table={TableName} toko={KodeToko} " +
                        "directory={DirectoryName} reason=invalid_tglProses_directory",
                        tableName,
                        kodeToko,
                        directoryName);

                    continue;
                }

                if (tglProsesDate >= cutoffDate)
                {
                    continue;
                }

                try
                {
                    Directory.Delete(
                        snapshotDirectory,
                        recursive: true);

                    deletedSnapshots++;

                    _logger.LogInformation(
                        "[MONITORING_FILE_BACKUP_RETENTION_DELETED] " +
                        "table={TableName} toko={KodeToko} " +
                        "tglProses={TglProses} " +
                        "tglProsesDate={TglProsesDate:yyyy-MM-dd} " +
                        "cutoffDate={CutoffDate:yyyy-MM-dd} " +
                        "path={Path}",
                        tableName,
                        kodeToko,
                        tglProses,
                        tglProsesDate,
                        cutoffDate,
                        snapshotDirectory);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "[MONITORING_FILE_BACKUP_RETENTION_DELETE_FAILED] " +
                        "table={TableName} toko={KodeToko} " +
                        "tglProses={TglProses} " +
                        "tglProsesDate={TglProsesDate:yyyy-MM-dd} " +
                        "path={Path}",
                        tableName,
                        kodeToko,
                        tglProses,
                        tglProsesDate,
                        snapshotDirectory);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "[MONITORING_FILE_BACKUP_RETENTION_TOKO_FAILED] " +
                "table={TableName} toko={KodeToko} path={Path}",
                tableName,
                kodeToko,
                tokoDirectory);
        }
    }

    private static bool TryGetTglProsesDate(
        string? directoryName,
        out long tglProses,
        out DateOnly tglProsesDate)
    {
        tglProses = 0;
        tglProsesDate = default;

        if (string.IsNullOrWhiteSpace(
                directoryName))
        {
            return false;
        }

        if (directoryName.Length < 8)
        {
            return false;
        }

        var datePart =
            directoryName[..8];

        if (!datePart.All(
                char.IsDigit))
        {
            return false;
        }

        if (!DateOnly.TryParseExact(
                datePart,
                "yyyyMMdd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out tglProsesDate))
        {
            return false;
        }

        if (!long.TryParse(
                directoryName,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out tglProses))
        {
            return false;
        }

        return true;
    }

    private static void ValidateSegment(
        string value,
        string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            value,
            parameterName);

        if (value is "." or "..")
        {
            throw new ArgumentException(
                $"Invalid path segment '{value}'.",
                parameterName);
        }

        if (value.Contains(
                Path.DirectorySeparatorChar) ||
            value.Contains(
                Path.AltDirectorySeparatorChar))
        {
            throw new ArgumentException(
                $"Path segment '{parameterName}' cannot contain directory separators.",
                parameterName);
        }

        if (value.IndexOfAny(
                Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException(
                $"Path segment '{parameterName}' contains invalid filename characters.",
                parameterName);
        }
    }

    private static void TryDeleteFile(
        string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Do not hide the original exception.
        }
    }
}

public sealed class MonitoringBackupOptions
{
    public string RootPath { get; set; } = string.Empty;

    public int RetentionDays { get; set; } = 1;
}

public sealed class FileMonitoringBatch
{
    public string TableName { get; set; } = string.Empty;

    public string KodeToko { get; set; } = string.Empty;

    public string JenisFile { get; set; } = string.Empty;

    public long TglProses { get; set; }

    public DateOnly TglDt3 { get; set; }

    public long BatchNumber { get; set; }

    public long RowCount { get; set; }

    public IReadOnlyList<
        Dictionary<string, JsonElement>> Data
    { get; set; } =
        [];
}

public sealed class FileMonitoringCompletion
{
    public string TableName { get; set; } = string.Empty;

    public string KodeToko { get; set; } = string.Empty;

    public string JenisFile { get; set; } = string.Empty;

    public long TglProses { get; set; }

    public DateOnly TglDt3 { get; set; }

    public long ExpectedRowCount { get; set; }

    public long RowCount { get; set; }

    public long BatchCount { get; set; }

    public DateTimeOffset CompletedAtUtc { get; set; }
}