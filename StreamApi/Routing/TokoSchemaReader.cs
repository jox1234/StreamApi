using Npgsql;
using StreamApi.PostgreSQL;

namespace StreamApi.Routing;

public sealed class TokoSchemaReader
{
    private readonly PostgreSqlConnectionFactory _connectionFactory;
    private readonly PostgreSqlDatabaseDiscovery _databaseDiscovery;

    public TokoSchemaReader(
        PostgreSqlConnectionFactory connectionFactory,
        PostgreSqlDatabaseDiscovery databaseDiscovery)
    {
        _connectionFactory = connectionFactory;
        _databaseDiscovery = databaseDiscovery;
    }

    public async Task<IReadOnlyList<TokoRoute>> ReadRoutesAsync(
        CancellationToken cancellationToken)
    {
        var posCatalogDatabase =
            await _databaseDiscovery.FindPosCatalogDatabaseAsync(
                cancellationToken);

        await _databaseDiscovery.VerifyTokoSchemaAsync(
                posCatalogDatabase,
                cancellationToken);

        const string sql = """
            SELECT
                kdtk,
                partition_id,
                database_name
            FROM public.toko_schema
            WHERE kdtk IS NOT NULL
              AND partition_id IS NOT NULL
            ORDER BY kdtk;
            """;

        await using var connection =
            _connectionFactory.CreateConnection(
                posCatalogDatabase);

        await connection.OpenAsync(cancellationToken);

        await using var command =
            new NpgsqlCommand(sql, connection);

        await using var reader =
            await command.ExecuteReaderAsync(cancellationToken);

        var routes = new List<TokoRoute>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var kodeToko = reader.GetString(0);
            var partitionId = reader.GetInt32(1);

            var databaseName =
                reader.IsDBNull(2)
                    ? string.Empty
                    : reader.GetString(2);

            routes.Add(new TokoRoute
            {
                KodeToko = kodeToko,
                PartitionId = partitionId,
                DatabaseName = databaseName
            });
        }

        return routes;
    }
}