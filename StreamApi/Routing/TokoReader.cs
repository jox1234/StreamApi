using Npgsql;
using StreamApi.PostgreSQL;

namespace StreamApi.Routing;

public sealed class TokoReader
{
    private readonly PostgreSqlConnectionFactory _connectionFactory;
    private readonly PostgreSqlDatabaseDiscovery _databaseDiscovery;

    public TokoReader(
        PostgreSqlConnectionFactory connectionFactory,
        PostgreSqlDatabaseDiscovery databaseDiscovery)
    {
        _connectionFactory = connectionFactory;
        _databaseDiscovery = databaseDiscovery;
    }

    public async Task<string?> GetBranchCodeAsync(
        string kodeToko,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            kodeToko);

        // ----------------------------------------------------
        // Find POSCatalog database
        // ----------------------------------------------------

        var posCatalogDatabase =
            await _databaseDiscovery.FindPosCatalogDatabaseAsync(
                cancellationToken);

        // ----------------------------------------------------
        // Lightweight query
        //
        // Toko.kdtk  -> storeCode
        // Toko.kirim -> branchCode
        // ----------------------------------------------------

        const string sql = """
            SELECT kirim
            FROM public."Toko"
            WHERE kdtk = @kdtk
            LIMIT 1;
            """;

        await using var connection =
            _connectionFactory.CreateConnection(
                posCatalogDatabase);

        await connection.OpenAsync(
            cancellationToken);

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        command.Parameters.AddWithValue(
            "kdtk",
            kodeToko);

        var result =
            await command.ExecuteScalarAsync(
                cancellationToken);

        if (result is null ||
            result == DBNull.Value)
        {
            return null;
        }

        return result.ToString();
    }
}