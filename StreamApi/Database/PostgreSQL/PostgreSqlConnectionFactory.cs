using Npgsql;

namespace StreamApi.PostgreSQL;

public sealed class PostgreSqlConnectionFactory
{
    private readonly string _serverConnectionString;

    public PostgreSqlConnectionFactory(
        IConfiguration configuration)
    {
        _serverConnectionString =
            configuration.GetConnectionString("PostgreSql")
            ?? throw new InvalidOperationException(
                "Connection string 'PostgreSql' is not configured.");
    }

    public NpgsqlConnection CreateServerConnection()
    {
        var builder =
            new NpgsqlConnectionStringBuilder(
                _serverConnectionString)
            {
                ApplicationName = "StreamApi"
            };

        return new NpgsqlConnection(
            builder.ConnectionString);
    }

    public NpgsqlConnection CreateConnection(
    string databaseName)
    {
        var builder =
            new NpgsqlConnectionStringBuilder(
                _serverConnectionString)
            {
                Database = databaseName,
                Pooling = false,
                ApplicationName = "StreamApi"
            };

        return new NpgsqlConnection(
            builder.ConnectionString);
    }

    public NpgsqlConnection CreateCatalogConnection()
    {
        var builder =
            new NpgsqlConnectionStringBuilder(
                _serverConnectionString)
            {
                Database = "POSCatalog_Beta",
                Pooling = true,
                ApplicationName = "StreamApi"
            };

        return new NpgsqlConnection(
            builder.ConnectionString);
    }
}