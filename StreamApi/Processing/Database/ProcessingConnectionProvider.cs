using StreamApi.PostgreSQL;
using StreamApi.Routing;

namespace StreamApi.Processing.Database;
public interface IProcessingConnectionProvider
{
    Task<ProcessingConnectionLease> OpenAsync(
        string databaseName,
        CancellationToken cancellationToken);
}
public sealed class ProcessingConnectionProvider
    : IProcessingConnectionProvider
{
    private readonly PostgreSqlConnectionFactory
        _connectionFactory;

    private readonly PartitionConnectionBudget
        _connectionBudget;

    public ProcessingConnectionProvider(
        PostgreSqlConnectionFactory connectionFactory,
        PartitionConnectionBudget connectionBudget)
    {
        _connectionFactory =
            connectionFactory;

        _connectionBudget =
            connectionBudget;
    }

    public async Task<ProcessingConnectionLease> OpenAsync(
        string databaseName,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new ArgumentException(
                "Database name is required.",
                nameof(databaseName));
        }

        var budgetLease =
            await _connectionBudget.AcquireAsync(
                cancellationToken);

        var connection =
            _connectionFactory.CreateConnection(
                databaseName);

        try
        {
            await connection.OpenAsync(
                cancellationToken);

            return new ProcessingConnectionLease(
                connection,
                budgetLease);
        }
        catch
        {
            await connection.DisposeAsync();

            budgetLease.Dispose();

            throw;
        }
    }
}