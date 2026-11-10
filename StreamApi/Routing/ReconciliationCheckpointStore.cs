using StreamApi.PostgreSQL;

namespace StreamApi.Routing;

public sealed class ReconciliationCheckpointStore
{
    private readonly object _sync = new();

    private readonly Dictionary<
        ReconciliationCheckpointKey,
        long> _checkpoints = new();

    private readonly PostgreSqlCheckpointStore
        _postgresqlCheckpointStore;

    public bool IsInitialized
    {
        get;
        private set;
    }

    public ReconciliationCheckpointStore(
        PostgreSqlCheckpointStore postgresqlCheckpointStore)
    {
        _postgresqlCheckpointStore =
            postgresqlCheckpointStore;
    }

    public bool TryGet(
        string kodeToko,
        string jenisFile,
        out long tglProses)
    {
        var key =
            new ReconciliationCheckpointKey(
                kodeToko,
                jenisFile);

        lock (_sync)
        {
            return _checkpoints.TryGetValue(
                key,
                out tglProses);
        }
    }

    public async Task AdvanceAsync(
        string kodeToko,
        string jenisFile,
        long tglProses,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            kodeToko);

        ArgumentException.ThrowIfNullOrWhiteSpace(
            jenisFile);

        lock (_sync)
        {
            var key =
                new ReconciliationCheckpointKey(
                    kodeToko,
                    jenisFile);

            if (_checkpoints.TryGetValue(
                    key,
                    out var current) &&
                tglProses <= current)
            {
                return;
            }
        }

        await _postgresqlCheckpointStore.SaveAsync(
            kodeToko,
            jenisFile,
            tglProses,
            cancellationToken);

        lock (_sync)
        {
            var key =
                new ReconciliationCheckpointKey(
                    kodeToko,
                    jenisFile);

            if (_checkpoints.TryGetValue(
                    key,
                    out var current))
            {
                if (tglProses <= current)
                {
                    return;
                }
            }

            _checkpoints[key] =
                tglProses;
        }
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

        await _postgresqlCheckpointStore
            .SeedBaselineAsync(
                kodeToko,
                jenisFile,
                tglProses,
                cancellationToken);

        /*
         * IMPORTANT:
         *
         * Baseline initialization must update RAM as well as
         * PostgreSQL. Otherwise the first reconciliation cycle
         * would still see an empty checkpoint snapshot and would
         * enqueue the historical events again.
         */
        lock (_sync)
        {
            var key =
                new ReconciliationCheckpointKey(
                    kodeToko,
                    jenisFile);

            if (_checkpoints.TryGetValue(
                    key,
                    out var current) &&
                tglProses <= current)
            {
                return;
            }

            _checkpoints[key] =
                tglProses;
        }
    }

    public IReadOnlyDictionary<
        ReconciliationCheckpointKey,
        long>
        Snapshot()
    {
        lock (_sync)
        {
            return new Dictionary<
                ReconciliationCheckpointKey,
                long>(
                _checkpoints);
        }
    }

    public long GetMinimumForToko(
        string kodeToko)
    {
        lock (_sync)
        {
            var found = false;
            var minimum = long.MaxValue;

            foreach (var checkpoint in _checkpoints)
            {
                if (!string.Equals(
                        checkpoint.Key.KodeToko,
                        kodeToko,
                        StringComparison.Ordinal))
                {
                    continue;
                }

                found = true;

                if (checkpoint.Value < minimum)
                {
                    minimum = checkpoint.Value;
                }
            }

            return found
                ? minimum
                : long.MinValue;
        }
    }

    public async Task LoadAsync(
        CancellationToken cancellationToken)
    {
        var checkpoints =
            await _postgresqlCheckpointStore.LoadAsync(
                cancellationToken);

        lock (_sync)
        {
            _checkpoints.Clear();

            foreach (var checkpoint in checkpoints)
            {
                var key =
                    new ReconciliationCheckpointKey(
                        checkpoint.KodeToko,
                        checkpoint.JenisFile);

                _checkpoints[key] =
                    checkpoint.TglProses;
            }
        }
    }

    public async Task<bool> IsInitializedAsync(
        CancellationToken cancellationToken)
    {
        return await _postgresqlCheckpointStore
            .IsInitializedAsync(
                cancellationToken);
    }

    public async Task MarkInitializedAsync(
        CancellationToken cancellationToken)
    {
        await _postgresqlCheckpointStore
            .MarkInitializedAsync(
                cancellationToken);

        IsInitialized = true;
    }

    public void MarkInitialized()
    {
        IsInitialized = true;
    }
}

public readonly record struct ReconciliationCheckpointKey(
    string KodeToko,
    string JenisFile);