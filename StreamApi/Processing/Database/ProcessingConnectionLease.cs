using Npgsql;

namespace StreamApi.Processing.Database;

public sealed class ProcessingConnectionLease
    : IAsyncDisposable
{
    private readonly NpgsqlConnection _connection;
    private readonly IDisposable _budgetLease;

    private int _disposed;

    public ProcessingConnectionLease(
        NpgsqlConnection connection,
        IDisposable budgetLease)
    {
        _connection = connection;
        _budgetLease = budgetLease;
    }

    public NpgsqlConnection Connection =>
        _connection;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(
                ref _disposed,
                1) != 0)
        {
            return;
        }

        await _connection.DisposeAsync();

        _budgetLease.Dispose();
    }
}