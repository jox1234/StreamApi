using Microsoft.Extensions.Logging;
using Npgsql;

namespace StreamApi.PostgreSQL;

public sealed class PostgreSqlDatabaseInitializer
{
    private readonly ILogger<PostgreSqlDatabaseInitializer>
        _logger;

    public PostgreSqlDatabaseInitializer(
        ILogger<PostgreSqlDatabaseInitializer> logger)
    {
        _logger = logger;
    }

    public async Task<bool> EnsureLogDt3TriggerAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[POSTGRES_TRIGGER_INSTALL_START] database={Database}",
            connection.Database);

        var exists =
            await HasLogDt3ProsesAsync(
                connection,
                cancellationToken);

        if (!exists)
        {
            _logger.LogInformation(
                "[POSTGRES_LOG_DT3_PROSES_UNAVAILABLE] database={Database} table=public.log_dt3_proses",
                connection.Database);

            return false;
        }

        try
        {
            await using var command =
                new NpgsqlCommand(
                    PostgreSqlTriggerDefinition.InstallSql,
                    connection);

            await command.ExecuteNonQueryAsync(
                cancellationToken);

            _logger.LogInformation(
                "[POSTGRES_TRIGGER_INSTALL_COMPLETE] database={Database} trigger={Trigger}",
                connection.Database,
                PostgreSqlTriggerDefinition.TriggerName);

            return true;
        }
        catch (PostgresException ex)
            when (ex.SqlState == "42P01")
        {
            _logger.LogInformation(
                "[POSTGRES_LOG_DT3_PROSES_UNAVAILABLE] database={Database} table=public.log_dt3_proses reason=relation_missing_during_trigger_install",
                connection.Database);

            return false;
        }
    }

    public async Task<bool> HasLogDt3ProsesAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT to_regclass('public.log_dt3_proses') IS NOT NULL;
            """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        var result =
            await command.ExecuteScalarAsync(
                cancellationToken);

        return result is bool exists &&
               exists;
    }

    public async Task EnsureProcessingCheckpointTableAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = """
            CREATE TABLE IF NOT EXISTS public.streamapi_processing_checkpoint
            (
                kode_toko varchar(10) NOT NULL,
                jenis_file varchar(100) NOT NULL,
                tgl_proses bigint NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now(),

                PRIMARY KEY (kode_toko, jenis_file)
            );
            """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        await command.ExecuteNonQueryAsync(
            cancellationToken);

        _logger.LogInformation(
            "[POSTGRES_CHECKPOINT_TABLE_READY] database={Database} table=public.streamapi_processing_checkpoint",
            connection.Database);
    }

    public async Task EnsureProcessingCheckpointStateTableAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = """
            CREATE TABLE IF NOT EXISTS public.streamapi_processing_checkpoint_state
            (
                id smallint NOT NULL,
                initialized_at timestamptz NOT NULL DEFAULT now(),

                PRIMARY KEY (id),

                CONSTRAINT streamapi_processing_checkpoint_state_singleton
                    CHECK (id = 1)
            );
            """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        await command.ExecuteNonQueryAsync(
            cancellationToken);

        _logger.LogInformation(
            "[POSTGRES_CHECKPOINT_STATE_TABLE_READY] database={Database} table=public.streamapi_processing_checkpoint_state",
            connection.Database);
    }

    public async Task EnsureProcessingEventStateTableAsync(
    NpgsqlConnection connection,
    CancellationToken cancellationToken)
    {
        const string sql = """
        CREATE TABLE IF NOT EXISTS public.streamapi_processing_event_state
        (
            kode_toko varchar(10) NOT NULL,
            jenis_file varchar(100) NOT NULL,
            tgl_proses bigint NOT NULL,

            batch_size integer NOT NULL,

            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),

            PRIMARY KEY
            (
                kode_toko,
                jenis_file,
                tgl_proses
            ),

            CONSTRAINT streamapi_processing_event_state_batch_size_positive
                CHECK (batch_size > 0)
        );
        """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        await command.ExecuteNonQueryAsync(
            cancellationToken);

        _logger.LogInformation(
            "[POSTGRES_PROCESSING_EVENT_STATE_TABLE_READY] database={Database} table=public.streamapi_processing_event_state",
            connection.Database);
    }

}