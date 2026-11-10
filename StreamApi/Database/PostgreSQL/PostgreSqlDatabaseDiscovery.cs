using Microsoft.Extensions.Logging;
using Npgsql;

namespace StreamApi.PostgreSQL;

public sealed class PostgreSqlDatabaseDiscovery
{
    private readonly PostgreSqlConnectionFactory _connectionFactory;
    private readonly ILogger<PostgreSqlDatabaseDiscovery> _logger;

    public PostgreSqlDatabaseDiscovery(
        PostgreSqlConnectionFactory connectionFactory,
        ILogger<PostgreSqlDatabaseDiscovery> logger)
    {
        _connectionFactory = connectionFactory;
        _logger = logger;
    }

    public async Task<string> FindPosCatalogDatabaseAsync(
        CancellationToken cancellationToken)
    {
        const string expectedDatabase = "POSCatalog_Beta";

        const string sql = """
            SELECT datname
            FROM pg_database
            WHERE datallowconn = true
              AND NOT datistemplate
              AND datname = 'POSCatalog_Beta';
            """;

        _logger.LogInformation(
            "[POSTGRES_CATALOG_DISCOVERY_START] database={Database}",
            expectedDatabase);

        await using var connection =
            _connectionFactory.CreateServerConnection();

        await connection.OpenAsync(cancellationToken);

        await using var command =
            new NpgsqlCommand(sql, connection);

        var databases = new List<string>();

        await using var reader =
            await command.ExecuteReaderAsync(
                cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            databases.Add(reader.GetString(0));
        }

        if (databases.Count == 0)
        {
            _logger.LogError(
                "[POSTGRES_CATALOG_NOT_FOUND] database={Database}",
                expectedDatabase);

            throw new InvalidOperationException(
                $"POSCatalog database '{expectedDatabase}' was not found.");
        }

        if (databases.Count > 1)
        {
            throw new InvalidOperationException(
                $"Multiple POSCatalog databases were found: " +
                $"{string.Join(", ", databases)}");
        }

        var database = databases[0];

        _logger.LogInformation(
            "[POSTGRES_CATALOG_DISCOVERY_COMPLETE] database={Database}",
            database);

        return database;
    }

    public async Task VerifyTokoSchemaAsync(
    string databaseName,
    CancellationToken cancellationToken)
    {
        const string sql = """
        SELECT EXISTS
        (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'toko_schema'
              AND table_type = 'BASE TABLE'
        );
        """;

        _logger.LogInformation(
            "[POSTGRES_TOKO_SCHEMA_CHECK_START] database={Database} schema=public table=toko_schema",
            databaseName);

        await using var connection =
            _connectionFactory.CreateConnection(
                databaseName);

        await connection.OpenAsync(cancellationToken);

        await using var command =
            new NpgsqlCommand(sql, connection);

        var exists =
            await command.ExecuteScalarAsync(
                cancellationToken);

        if (exists is not true)
        {
            _logger.LogError(
                "[POSTGRES_TOKO_SCHEMA_NOT_FOUND] database={Database} schema=public table=toko_schema",
                databaseName);

            throw new InvalidOperationException(
                $"Table 'public.toko_schema' was not found in database '{databaseName}'.");
        }

        _logger.LogInformation(
            "[POSTGRES_TOKO_SCHEMA_CHECK_COMPLETE] database={Database} schema=public table=toko_schema",
            databaseName);
    }
}