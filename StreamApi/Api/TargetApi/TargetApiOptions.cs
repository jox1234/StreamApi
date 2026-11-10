namespace StreamApi.Api.TargetApi;

public sealed class TargetApiOptions
{
    public string BaseUrl { get; set; } = string.Empty;

    public string LoginPath { get; set; } =
        "/api/Auth/Login";

    public string TargetPath { get; set; } =
        "/api/MonitorProdmast/ReceiveTfProdmast";

    public string Username { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;
}