namespace StreamApi.Api.TargetApi;

public sealed class TargetApiData
{
    public string KodeToko { get; set; } = string.Empty;

    public string Table { get; set; } = string.Empty;

    public long DataServer { get; set; }

    public DateTime TanggalProses { get; set; }

    public DateOnly TanggalDT3 { get; set; }
}