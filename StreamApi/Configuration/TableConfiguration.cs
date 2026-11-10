namespace StreamApi.Configuration;

public sealed class TableConfiguration
{
    public string Name { get; set; } = string.Empty;

    public string Schema { get; set; } = "public";

    public bool Enabled { get; set; }

    public bool UseKodeTokoMapping { get; set; }

    public string SourceTableBaseName { get; set; } = string.Empty;
}