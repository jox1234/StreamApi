using Microsoft.Extensions.Logging;
using Npgsql;
using StreamApi.Routing;

namespace StreamApi.PostgreSQL;

public sealed class PostgreSqlLogDt3Reader
{
    private readonly ILogger<PostgreSqlLogDt3Reader>
        _logger;

    public PostgreSqlLogDt3Reader(
        ILogger<PostgreSqlLogDt3Reader> logger)
    {
        _logger = logger;
    }

    public async Task<IReadOnlyList<LogDt3Row>> ReadNewRowsAsync(
        NpgsqlConnection connection,
        PartitionMonitorSnapshot partition,
        IReadOnlyDictionary<
            ReconciliationCheckpointKey,
            long> checkpoints,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            partition);

        ArgumentNullException.ThrowIfNull(
            checkpoints);

        if (partition.TokoRoutes.Count == 0)
        {
            return Array.Empty<LogDt3Row>();
        }

        const string existsSql = """
            SELECT to_regclass('public.log_dt3_proses') IS NOT NULL;
            """;

        await using (
            var existsCommand =
                new NpgsqlCommand(
                    existsSql,
                    connection))
        {
            var exists =
                await existsCommand.ExecuteScalarAsync(
                    cancellationToken);

            if (exists is not true)
            {
                _logger.LogDebug(
                    "[LOG_DT3_PROSES_NOT_AVAILABLE] partition={PartitionId} database={Database}",
                    partition.PartitionId,
                    connection.Database);

                return Array.Empty<LogDt3Row>();
            }
        }

        // ------------------------------------------------------------
        // Toko routing values
        // ------------------------------------------------------------

        var tokoValues =
            new List<string>();

        var parameters =
            new List<NpgsqlParameter>();

        for (var i = 0;
             i < partition.TokoRoutes.Count;
             i++)
        {
            var toko =
                partition.TokoRoutes[i];

            var tokoParameter =
                $"@toko{i}";

            tokoValues.Add(
                $"({tokoParameter})");

            parameters.Add(
                new NpgsqlParameter(
                    tokoParameter,
                    toko.KodeToko));
        }

        // ------------------------------------------------------------
        // Per-stream checkpoints
        // ------------------------------------------------------------

        var partitionTokoSet =
            partition.TokoRoutes
                .Select(x => x.KodeToko)
                .ToHashSet(
                    StringComparer.Ordinal);

        var checkpointValues =
            new List<string>();

        if (checkpoints.Count > 0)
        {
            var checkpointIndex = 0;

            foreach (var checkpoint in checkpoints)
            {
                if (!partitionTokoSet.Contains(
                        checkpoint.Key.KodeToko))
                {
                    continue;
                }

                var kodeTokoParameter =
                    $"@checkpointToko{checkpointIndex}";

                var jenisFileParameter =
                    $"@checkpointJenis{checkpointIndex}";

                var checkpointParameter =
                    $"@checkpointValue{checkpointIndex}";

                checkpointValues.Add(
                    $"({kodeTokoParameter}, " +
                    $"{jenisFileParameter}, " +
                    $"{checkpointParameter})");

                parameters.Add(
                    new NpgsqlParameter(
                        kodeTokoParameter,
                        checkpoint.Key.KodeToko));

                parameters.Add(
                    new NpgsqlParameter(
                        jenisFileParameter,
                        checkpoint.Key.JenisFile));

                parameters.Add(
                    new NpgsqlParameter(
                        checkpointParameter,
                        checkpoint.Value));

                checkpointIndex++;
            }
        }

        // ------------------------------------------------------------
        // Checkpoint join
        // ------------------------------------------------------------

        var checkpointJoin =
            checkpointValues.Count > 0
                ? $"""
                  LEFT JOIN
                  (
                      VALUES
                          {string.Join(",\n", checkpointValues)}
                  ) AS c(kode_toko, jenis_file, checkpoint)
                      ON c.kode_toko = l.kode_toko
                     AND c.jenis_file = l.jenis_file
                  """
                : string.Empty;

        var checkpointPredicate =
            checkpointValues.Count > 0
                ? """
                  AND
                  (
                      c.checkpoint IS NULL
                      OR l.tgl_proses > c.checkpoint
                  )
                  """
                : string.Empty;

        // ------------------------------------------------------------
        // SQL
        // ------------------------------------------------------------

        var sql =
            $"""
            SELECT
                l.kode_toko,
                l.jenis_file,
                l.tgl_dt3,
                l.jumlah_record,
                l.tgl_proses
            FROM public.log_dt3_proses AS l
            INNER JOIN
            (
                VALUES
                    {string.Join(",\n", tokoValues)}
            ) AS t(kode_toko)
                ON t.kode_toko = l.kode_toko
            {checkpointJoin}
            WHERE 1 = 1
                {checkpointPredicate}
            ORDER BY
                l.kode_toko,
                l.jenis_file,
                l.tgl_proses;
            """;

        // ------------------------------------------------------------
        // Execute
        // ------------------------------------------------------------

        var rows =
            new List<LogDt3Row>();

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        command.Parameters.AddRange(
            parameters.ToArray());

        await using var reader =
            await command.ExecuteReaderAsync(
                cancellationToken);

        while (await reader.ReadAsync(
                   cancellationToken))
        {
            rows.Add(
                new LogDt3Row(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetFieldValue<DateOnly>(2),
                    reader.GetInt32(3),
                    reader.GetInt64(4)));
        }

        _logger.LogDebug(
            "[LOG_DT3_RECONCILIATION_READ] partition={PartitionId} database={Database} toko={TokoCount} checkpoints={CheckpointCount} detected={Detected}",
            partition.PartitionId,
            connection.Database,
            partition.TokoRoutes.Count,
            checkpointValues.Count,
            rows.Count);

        return rows;
    }

    public async Task<IReadOnlyList<LogDt3Row>> ReadLatestRowsAsync(
        NpgsqlConnection connection,
        PartitionMonitorSnapshot partition,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            partition);

        if (partition.TokoRoutes.Count == 0)
        {
            return Array.Empty<LogDt3Row>();
        }

        const string existsSql = """
            SELECT to_regclass('public.log_dt3_proses') IS NOT NULL;
            """;

        await using (
            var existsCommand =
                new NpgsqlCommand(
                    existsSql,
                    connection))
        {
            var exists =
                await existsCommand.ExecuteScalarAsync(
                    cancellationToken);

            if (exists is not true)
            {
                _logger.LogDebug(
                    "[LOG_DT3_PROSES_NOT_AVAILABLE] partition={PartitionId} database={Database}",
                    partition.PartitionId,
                    connection.Database);

                return Array.Empty<LogDt3Row>();
            }
        }

        // ------------------------------------------------------------
        // Toko routing values
        // ------------------------------------------------------------

        var tokoValues =
            new List<string>();

        var parameters =
            new List<NpgsqlParameter>();

        for (var i = 0;
             i < partition.TokoRoutes.Count;
             i++)
        {
            var tokoParameter =
                $"@toko{i}";

            tokoValues.Add(
                $"({tokoParameter})");

            parameters.Add(
                new NpgsqlParameter(
                    tokoParameter,
                    partition.TokoRoutes[i].KodeToko));
        }

        // ------------------------------------------------------------
        // SQL
        // ------------------------------------------------------------

        var sql =
            $"""
            WITH toko_filter(kode_toko) AS
            (
                VALUES
                    {string.Join(",\n", tokoValues)}
            )
            SELECT
                l.kode_toko,
                l.jenis_file,
                l.tgl_dt3,
                l.jumlah_record,
                l.tgl_proses
            FROM public.log_dt3_proses AS l
            INNER JOIN toko_filter AS t
                ON t.kode_toko = l.kode_toko
            INNER JOIN
            (
                SELECT
                    l2.kode_toko,
                    l2.jenis_file,
                    MAX(l2.tgl_proses) AS max_tgl_proses
                FROM public.log_dt3_proses AS l2
                INNER JOIN toko_filter AS t2
                    ON t2.kode_toko = l2.kode_toko
                GROUP BY
                    l2.kode_toko,
                    l2.jenis_file
            ) AS latest
                ON latest.kode_toko = l.kode_toko
               AND latest.jenis_file = l.jenis_file
               AND latest.max_tgl_proses = l.tgl_proses
            ORDER BY
                l.kode_toko,
                l.jenis_file;
            """;

        // ------------------------------------------------------------
        // Execute
        // ------------------------------------------------------------

        var rows =
            new List<LogDt3Row>();

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        command.Parameters.AddRange(
            parameters.ToArray());

        await using var reader =
            await command.ExecuteReaderAsync(
                cancellationToken);

        while (await reader.ReadAsync(
                   cancellationToken))
        {
            rows.Add(
                new LogDt3Row(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetFieldValue<DateOnly>(2),
                    reader.GetInt32(3),
                    reader.GetInt64(4)));
        }

        _logger.LogDebug(
            "[LOG_DT3_BASELINE_READ] partition={PartitionId} database={Database} toko={TokoCount} latestEvents={LatestEvents}",
            partition.PartitionId,
            connection.Database,
            partition.TokoRoutes.Count,
            rows.Count);

        return rows;
    }

    public async Task<IReadOnlyList<LogDt3Row>> ReadRetainedRowsAsync(
        NpgsqlConnection connection,
        PartitionMonitorSnapshot partition,
        DateOnly cutoffDate,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            connection);

        ArgumentNullException.ThrowIfNull(
            partition);

        cancellationToken.ThrowIfCancellationRequested();

        const string existsSql = """
            SELECT to_regclass('public.log_dt3_proses') IS NOT NULL;
            """;

        await using (
            var existsCommand =
                new NpgsqlCommand(
                    existsSql,
                    connection))
        {
            var exists =
                await existsCommand.ExecuteScalarAsync(
                    cancellationToken);

            if (exists is not true)
            {
                _logger.LogDebug(
                    "[INQUIRY_LOG_DT3_PROSES_NOT_AVAILABLE] " +
                    "partition={PartitionId} " +
                    "database={Database}",
                    partition.PartitionId,
                    connection.Database);

                return Array.Empty<LogDt3Row>();
            }
        }

        /*
         * tgl_proses contains 18 digits:
         *
         * yyyyMMddHHmmssffff
         *
         * Therefore the beginning of a date is:
         *
         * yyyyMMdd0000000000
         *
         * Example:
         *
         * 202609210000000000
         *
         * There are TEN zeroes after yyyyMMdd.
         */
        var cutoffTglProses =
            long.Parse(
                $"{cutoffDate:yyyyMMdd}0000000000");

        const string sql = """
            SELECT
                l.kode_toko,
                l.jenis_file,
                l.tgl_dt3,
                l.jumlah_record,
                l.tgl_proses
            FROM public.log_dt3_proses AS l
            WHERE l.tgl_proses >= @cutoffTglProses
            ORDER BY
                l.tgl_proses,
                l.kode_toko,
                l.jenis_file;
            """;

        var rows =
            new List<LogDt3Row>();

        await using var command =
            new NpgsqlCommand(
                sql,
                connection)
            {
                /*
                 * Inquiry bootstrap is a background recovery query.
                 * It must not be killed by Npgsql's default 30-second
                 * command timeout when retained history is large.
                 */
                CommandTimeout = 0
            };

        command.Parameters.Add(
            new NpgsqlParameter(
                "@cutoffTglProses",
                cutoffTglProses));

        await using var reader =
            await command.ExecuteReaderAsync(
                cancellationToken);

        while (await reader.ReadAsync(
                   cancellationToken))
        {
            rows.Add(
                new LogDt3Row(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetFieldValue<DateOnly>(2),
                    reader.GetInt32(3),
                    reader.GetInt64(4)));
        }

        _logger.LogInformation(
            "[INQUIRY_LOG_DT3_RETAINED_READ] " +
            "partition={PartitionId} " +
            "database={Database} " +
            "cutoffDate={CutoffDate} " +
            "cutoffTglProses={CutoffTglProses} " +
            "rows={RowCount}",
            partition.PartitionId,
            connection.Database,
            cutoffDate,
            cutoffTglProses,
            rows.Count);

        return rows;
    }
}

public sealed record LogDt3Row(
    string KodeToko,
    string JenisFile,
    DateOnly TglDt3,
    int JumlahRecord,
    long TglProses);