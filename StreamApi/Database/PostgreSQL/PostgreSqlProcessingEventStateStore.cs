using Microsoft.Extensions.Logging;
using Npgsql;

namespace StreamApi.PostgreSQL;

public sealed class PostgreSqlProcessingEventStateStore
{
    private readonly PostgreSqlConnectionFactory
        _connectionFactory;

    private readonly ILogger<PostgreSqlProcessingEventStateStore>
        _logger;

    public PostgreSqlProcessingEventStateStore(
        PostgreSqlConnectionFactory connectionFactory,
        ILogger<PostgreSqlProcessingEventStateStore> logger)
    {
        _connectionFactory =
            connectionFactory;

        _logger =
            logger;
    }

    // ------------------------------------------------------------
    // Get existing batch size or create it
    //
    // The durable state table is initialized once during
    // application startup by PostgreSqlDatabaseInitializer.
    //
    // This method intentionally does NOT perform DDL.
    // ------------------------------------------------------------

    public async Task<int> GetOrCreateBatchSizeAsync(
        string kodeToko,
        string jenisFile,
        long tglProses,
        int proposedBatchSize,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            kodeToko);

        ArgumentException.ThrowIfNullOrWhiteSpace(
            jenisFile);

        if (proposedBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(proposedBatchSize),
                proposedBatchSize,
                "Batch size must be greater than zero.");
        }

        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

        const string insertSql = """
            INSERT INTO public.streamapi_processing_event_state
            (
                kode_toko,
                jenis_file,
                tgl_proses,
                batch_size
            )
            VALUES
            (
                @kode_toko,
                @jenis_file,
                @tgl_proses,
                @batch_size
            )
            ON CONFLICT
            (
                kode_toko,
                jenis_file,
                tgl_proses
            )
            DO NOTHING;
            """;

        await using (
            var insertCommand =
                new NpgsqlCommand(
                    insertSql,
                    connection))
        {
            insertCommand.Parameters.AddWithValue(
                "kode_toko",
                kodeToko);

            insertCommand.Parameters.AddWithValue(
                "jenis_file",
                jenisFile);

            insertCommand.Parameters.AddWithValue(
                "tgl_proses",
                tglProses);

            insertCommand.Parameters.AddWithValue(
                "batch_size",
                proposedBatchSize);

            await insertCommand.ExecuteNonQueryAsync(
                cancellationToken);
        }

        const string selectSql = """
            SELECT batch_size
            FROM public.streamapi_processing_event_state
            WHERE kode_toko = @kode_toko
              AND jenis_file = @jenis_file
              AND tgl_proses = @tgl_proses;
            """;

        await using var selectCommand =
            new NpgsqlCommand(
                selectSql,
                connection);

        selectCommand.Parameters.AddWithValue(
            "kode_toko",
            kodeToko);

        selectCommand.Parameters.AddWithValue(
            "jenis_file",
            jenisFile);

        selectCommand.Parameters.AddWithValue(
            "tgl_proses",
            tglProses);

        var result =
            await selectCommand.ExecuteScalarAsync(
                cancellationToken);

        if (result is null)
        {
            throw new InvalidOperationException(
                $"Unable to load processing event state for " +
                $"{kodeToko}/{jenisFile}/{tglProses}.");
        }

        var batchSize =
            Convert.ToInt32(result);

        if (batchSize <= 0)
        {
            throw new InvalidOperationException(
                $"Stored batch size is invalid for " +
                $"{kodeToko}/{jenisFile}/{tglProses}: {batchSize}.");
        }

        _logger.LogDebug(
            "[PROCESSING_EVENT_BATCH_SIZE_LOADED] toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses} batchSize={BatchSize}",
            kodeToko,
            jenisFile,
            tglProses,
            batchSize);

        return batchSize;
    }

    // ------------------------------------------------------------
    // Delete state after successful event completion
    //
    // The durable state table is initialized once during
    // application startup by PostgreSqlDatabaseInitializer.
    //
    // This method intentionally does NOT perform DDL.
    // ------------------------------------------------------------

    public async Task DeleteAsync(
        string kodeToko,
        string jenisFile,
        long tglProses,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            kodeToko);

        ArgumentException.ThrowIfNullOrWhiteSpace(
            jenisFile);

        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

        const string sql = """
            DELETE FROM public.streamapi_processing_event_state
            WHERE kode_toko = @kode_toko
              AND jenis_file = @jenis_file
              AND tgl_proses = @tgl_proses;
            """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        command.Parameters.AddWithValue(
            "kode_toko",
            kodeToko);

        command.Parameters.AddWithValue(
            "jenis_file",
            jenisFile);

        command.Parameters.AddWithValue(
            "tgl_proses",
            tglProses);

        await command.ExecuteNonQueryAsync(
            cancellationToken);

        _logger.LogDebug(
            "[PROCESSING_EVENT_STATE_DELETED] toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
            kodeToko,
            jenisFile,
            tglProses);
    }
}