using Npgsql;
using StreamApi.Routing;

namespace StreamApi.PostgreSQL;

public sealed class PostgreSqlCheckpointStore
{
    private readonly PostgreSqlConnectionFactory
        _connectionFactory;

    private readonly ILogger<PostgreSqlCheckpointStore>
        _logger;

    public PostgreSqlCheckpointStore(
        PostgreSqlConnectionFactory connectionFactory,
        ILogger<PostgreSqlCheckpointStore> logger)
    {
        _connectionFactory =
            connectionFactory;

        _logger =
            logger;
    }

    public async Task<
        IReadOnlyList<ReconciliationCheckpointRecord>>
        LoadAsync(
            CancellationToken cancellationToken)
    {
        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

        const string sql = """
            SELECT
                kode_toko,
                jenis_file,
                tgl_proses
            FROM public.streamapi_processing_checkpoint;
            """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        var results =
            new List<ReconciliationCheckpointRecord>();

        await using var reader =
            await command.ExecuteReaderAsync(
                cancellationToken);

        while (await reader.ReadAsync(
                   cancellationToken))
        {
            results.Add(
                new ReconciliationCheckpointRecord(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetInt64(2)));
        }

        _logger.LogInformation(
            "[POSTGRES_CHECKPOINT_LOAD_COMPLETE] database={Database} count={Count}",
            connection.Database,
            results.Count);

        return results;
    }

    public async Task SaveAsync(
        string kodeToko,
        string jenisFile,
        long tglProses,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            kodeToko);

        ArgumentException.ThrowIfNullOrWhiteSpace(
            jenisFile);

        const string sql = """
            INSERT INTO public.streamapi_processing_checkpoint
            (
                kode_toko,
                jenis_file,
                tgl_proses
            )
            VALUES
            (
                @kode_toko,
                @jenis_file,
                @tgl_proses
            )
            ON CONFLICT (kode_toko, jenis_file)
            DO UPDATE
            SET
                tgl_proses =
                    GREATEST(
                        public.streamapi_processing_checkpoint.tgl_proses,
                        EXCLUDED.tgl_proses
                    ),
                updated_at = now();
            """;

        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

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
            "[POSTGRES_CHECKPOINT_SAVED] toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
            kodeToko,
            jenisFile,
            tglProses);
    }

    public async Task<bool> IsInitializedAsync(
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT EXISTS
            (
                SELECT 1
                FROM public.streamapi_processing_checkpoint_state
                WHERE id = 1
            );
            """;

        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        var result =
            await command.ExecuteScalarAsync(
                cancellationToken);

        return result is bool initialized &&
               initialized;
    }

    public async Task MarkInitializedAsync(
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO public.streamapi_processing_checkpoint_state
            (
                id,
                initialized_at
            )
            VALUES
            (
                1,
                now()
            )
            ON CONFLICT (id)
            DO NOTHING;
            """;

        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        await command.ExecuteNonQueryAsync(
            cancellationToken);

        _logger.LogInformation(
            "[POSTGRES_CHECKPOINT_INITIALIZED]");
    }

    public async Task SeedBaselineAsync(
        string kodeToko,
        string jenisFile,
        long tglProses,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            kodeToko);

        ArgumentException.ThrowIfNullOrWhiteSpace(
            jenisFile);

        const string sql = """
            INSERT INTO public.streamapi_processing_checkpoint
            (
                kode_toko,
                jenis_file,
                tgl_proses
            )
            VALUES
            (
                @kode_toko,
                @jenis_file,
                @tgl_proses
            )
            ON CONFLICT (kode_toko, jenis_file)
            DO UPDATE
            SET
                tgl_proses =
                    GREATEST(
                        public.streamapi_processing_checkpoint.tgl_proses,
                        EXCLUDED.tgl_proses
                    ),
                updated_at = now();
            """;

        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

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
            "[POSTGRES_CHECKPOINT_BASELINE_SEEDED] toko={KodeToko} jenisFile={JenisFile} tglProses={TglProses}",
            kodeToko,
            jenisFile,
            tglProses);
    }
}

public sealed record ReconciliationCheckpointRecord(
    string KodeToko,
    string JenisFile,
    long TglProses);