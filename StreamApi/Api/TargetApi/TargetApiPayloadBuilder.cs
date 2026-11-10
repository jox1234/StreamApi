using System.Globalization;
using StreamApi.Api.Models;
using StreamApi.Routing;

namespace StreamApi.Api.TargetApi;

public sealed class TargetApiPayloadBuilder
{
    public IReadOnlyList<TargetApiData> Build(
        ReconciliationEvent reconciliationEvent,
        string targetTable,
        long dataServer)
    {
        var tanggalProses =
            ConvertTanggalProses(
                reconciliationEvent.TglProses);

        return new[]
        {
            new TargetApiData
            {
                KodeToko = reconciliationEvent.KodeToko,
                Table = targetTable,
                DataServer = dataServer,
                TanggalProses = tanggalProses,
                TanggalDT3 = reconciliationEvent.TglDt3
            }
        };
    }

    private static DateTime ConvertTanggalProses(long tglProses)
    {
        var value = tglProses.ToString(CultureInfo.InvariantCulture);

        if (DateTime.TryParseExact(
                value,
                "yyyyMMddHHmmssffff",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var result))
        {
            return result;
        }

        throw new FormatException(
            $"Invalid TglProses value: {tglProses}. " +
            "Expected format yyyyMMddHHmmssffff.");
    }
}