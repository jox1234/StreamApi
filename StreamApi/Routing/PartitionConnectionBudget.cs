using Npgsql;
using StreamApi.PostgreSQL;

namespace StreamApi.Routing;

public sealed class PartitionConnectionBudget
{
    private readonly PostgreSqlConnectionFactory _connectionFactory;
    private readonly ILogger<PartitionConnectionBudget> _logger;

    private readonly object _sync = new();

    private SemaphoreSlim? _semaphore;
    private SemaphoreSlim? _monitorSemaphore;

    private int _capacity;
    private int _monitorCapacity;

    private bool _initialized;

    public PartitionConnectionBudget(
        PostgreSqlConnectionFactory connectionFactory,
        ILogger<PartitionConnectionBudget> logger)
    {
        _connectionFactory =
            connectionFactory;

        _logger =
            logger;
    }

    public int Capacity
    {
        get
        {
            lock (_sync)
            {
                return _capacity;
            }
        }
    }

    public int MonitorCapacity
    {
        get
        {
            lock (_sync)
            {
                return _monitorCapacity;
            }
        }
    }

    public int ActiveConnections
    {
        get
        {
            lock (_sync)
            {
                if (_semaphore is null)
                {
                    return 0;
                }

                return
                    _capacity -
                    _semaphore.CurrentCount;
            }
        }
    }

    public int ActiveMonitorConnections
    {
        get
        {
            lock (_sync)
            {
                if (_monitorSemaphore is null)
                {
                    return 0;
                }

                return
                    _monitorCapacity -
                    _monitorSemaphore.CurrentCount;
            }
        }
    }

    public void Initialize(
        CancellationToken cancellationToken)
    {
        InitializeAsync(
            cancellationToken)
            .GetAwaiter()
            .GetResult();
    }

    public async Task InitializeAsync(
        CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (_initialized)
            {
                return;
            }
        }

        var statistics =
            await ReadPostgreSqlConnectionStatisticsAsync(
                cancellationToken);

        var maxConnections =
            statistics.MaxConnections;

        var superuserReserved =
            statistics.SuperuserReserved;

        var currentConnections =
            statistics.CurrentConnections;

        var safetyReserve =
            Math.Max(
                10,
                (int)Math.Ceiling(
                    maxConnections * 0.20));

        var available =
            Math.Max(
                0,
                maxConnections -
                superuserReserved -
                currentConnections -
                safetyReserve);

        var theoreticalCapacity =
            Math.Min(
                available,
                (int)Math.Floor(
                    maxConnections * 0.50));

        var startupCapacity =
            Math.Max(
                1,
                Math.Min(
                    theoreticalCapacity,
                    8));

        /*
         * IMPORTANT:
         *
         * One connection slot is permanently reserved
         * for non-monitor work such as reconciliation.
         *
         * Example:
         *
         *     total capacity       = 8
         *     monitor capacity     = 7
         *     reconciliation slot  = 1
         *
         * Therefore persistent LISTEN connections can
         * never consume the entire shared budget.
         */
        var monitorCapacity =
            Math.Max(
                1,
                startupCapacity - 1);

        var semaphore =
            new SemaphoreSlim(
                startupCapacity,
                startupCapacity);

        var monitorSemaphore =
            new SemaphoreSlim(
                monitorCapacity,
                monitorCapacity);

        lock (_sync)
        {
            if (_initialized)
            {
                semaphore.Dispose();
                monitorSemaphore.Dispose();

                return;
            }

            _capacity =
                startupCapacity;

            _monitorCapacity =
                monitorCapacity;

            _semaphore =
                semaphore;

            _monitorSemaphore =
                monitorSemaphore;

            _initialized =
                true;
        }

        _logger.LogInformation(
            "[POSTGRES_CONNECTION_BUDGET] max={MaxConnections} superuserReserved={SuperuserReserved} current={CurrentConnections} safetyReserve={SafetyReserve} available={Available} theoreticalCapacity={TheoreticalCapacity} startupCapacity={StartupCapacity} monitorCapacity={MonitorCapacity} reservedForReconciliation={ReservedForReconciliation}",
            maxConnections,
            superuserReserved,
            currentConnections,
            safetyReserve,
            available,
            theoreticalCapacity,
            startupCapacity,
            monitorCapacity,
            startupCapacity - monitorCapacity);
    }

    public async Task<IDisposable> AcquireAsync(
        CancellationToken cancellationToken)
    {
        var semaphore =
            GetSemaphore();

        await semaphore.WaitAsync(
            cancellationToken);

        return new ConnectionLease(
            semaphore);
    }

    public async Task<IDisposable?> TryAcquireAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var semaphore =
            GetSemaphore();

        var acquired =
            await semaphore.WaitAsync(
                timeout,
                cancellationToken);

        if (!acquired)
        {
            return null;
        }

        return new ConnectionLease(
            semaphore);
    }

    /*
     * Monitor-specific admission.
     *
     * A monitor must acquire BOTH:
     *
     * 1. a monitor slot
     * 2. a global PostgreSQL slot
     *
     * Since monitorCapacity = capacity - 1,
     * at least one global slot remains available
     * for reconciliation.
     */
    public async Task<IDisposable> AcquireMonitorAsync(
        CancellationToken cancellationToken)
    {
        var monitorSemaphore =
            GetMonitorSemaphore();

        await monitorSemaphore.WaitAsync(
            cancellationToken);

        var globalSemaphore =
            GetSemaphore();

        try
        {
            await globalSemaphore.WaitAsync(
                cancellationToken);

            return new MonitorConnectionLease(
                globalSemaphore,
                monitorSemaphore);
        }
        catch
        {
            monitorSemaphore.Release();
            throw;
        }
    }

    public async Task<IDisposable?> TryAcquireMonitorAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var monitorSemaphore =
            GetMonitorSemaphore();

        var monitorAcquired =
            await monitorSemaphore.WaitAsync(
                timeout,
                cancellationToken);

        if (!monitorAcquired)
        {
            return null;
        }

        var globalSemaphore =
            GetSemaphore();

        try
        {
            var globalAcquired =
                await globalSemaphore.WaitAsync(
                    timeout,
                    cancellationToken);

            if (!globalAcquired)
            {
                monitorSemaphore.Release();

                return null;
            }

            return new MonitorConnectionLease(
                globalSemaphore,
                monitorSemaphore);
        }
        catch
        {
            monitorSemaphore.Release();
            throw;
        }
    }

    private SemaphoreSlim GetSemaphore()
    {
        lock (_sync)
        {
            if (!_initialized ||
                _semaphore is null)
            {
                throw new InvalidOperationException(
                    "PartitionConnectionBudget has not been initialized.");
            }

            return _semaphore;
        }
    }

    private SemaphoreSlim GetMonitorSemaphore()
    {
        lock (_sync)
        {
            if (!_initialized ||
                _monitorSemaphore is null)
            {
                throw new InvalidOperationException(
                    "PartitionConnectionBudget has not been initialized.");
            }

            return _monitorSemaphore;
        }
    }

    private async Task<
        PostgreSqlConnectionStatistics>
        ReadPostgreSqlConnectionStatisticsAsync(
            CancellationToken cancellationToken)
    {
        await using var connection =
            _connectionFactory.CreateCatalogConnection();

        await connection.OpenAsync(
            cancellationToken);

        const string maxConnectionsSql = """
            SHOW max_connections;
            """;

        const string superuserReservedSql = """
            SHOW superuser_reserved_connections;
            """;

        const string currentConnectionsSql = """
            SELECT COUNT(*)
            FROM pg_stat_activity;
            """;

        var maxConnections =
            await ExecuteIntAsync(
                connection,
                maxConnectionsSql,
                cancellationToken);

        var superuserReserved =
            await ExecuteIntAsync(
                connection,
                superuserReservedSql,
                cancellationToken);

        var currentConnections =
            await ExecuteIntAsync(
                connection,
                currentConnectionsSql,
                cancellationToken);

        return new PostgreSqlConnectionStatistics(
            maxConnections,
            superuserReserved,
            currentConnections);
    }

    private static async Task<int> ExecuteIntAsync(
        NpgsqlConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        var value =
            await command.ExecuteScalarAsync(
                cancellationToken);

        return Convert.ToInt32(value);
    }

    private sealed class ConnectionLease
        : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;
        private int _disposed;

        public ConnectionLease(
            SemaphoreSlim semaphore)
        {
            _semaphore =
                semaphore;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(
                    ref _disposed,
                    1) != 0)
            {
                return;
            }

            _semaphore.Release();
        }
    }

    private sealed class MonitorConnectionLease
        : IDisposable
    {
        private readonly SemaphoreSlim _globalSemaphore;
        private readonly SemaphoreSlim _monitorSemaphore;

        private int _disposed;

        public MonitorConnectionLease(
            SemaphoreSlim globalSemaphore,
            SemaphoreSlim monitorSemaphore)
        {
            _globalSemaphore =
                globalSemaphore;

            _monitorSemaphore =
                monitorSemaphore;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(
                    ref _disposed,
                    1) != 0)
            {
                return;
            }

            /*
             * Release the global slot first,
             * then the monitor-specific slot.
             */
            _globalSemaphore.Release();

            _monitorSemaphore.Release();
        }
    }

    private readonly record struct
        PostgreSqlConnectionStatistics(
            int MaxConnections,
            int SuperuserReserved,
            int CurrentConnections);
}