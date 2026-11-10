using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Options;

namespace StreamApi.Api.MonitoringToko;

public sealed class MonitoringTokoBasicAuthentication
{
    private readonly MonitoringTokoOptions _options;

    public MonitoringTokoBasicAuthentication(
        IOptions<MonitoringTokoOptions> options)
    {
        _options = options.Value;
    }

    public bool IsValid(
        string? authorizationHeader)
    {
        if (string.IsNullOrWhiteSpace(
                authorizationHeader))
        {
            return false;
        }

        if (!AuthenticationHeaderValue.TryParse(
                authorizationHeader,
                out var header))
        {
            return false;
        }

        if (!string.Equals(
                header.Scheme,
                "Basic",
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(
                header.Parameter))
        {
            return false;
        }

        try
        {
            var decoded =
                Encoding.UTF8.GetString(
                    Convert.FromBase64String(
                        header.Parameter));

            var separator =
                decoded.IndexOf(':');

            if (separator <= 0)
            {
                return false;
            }

            var username =
                decoded[..separator];

            var password =
                decoded[(separator + 1)..];

            return
                string.Equals(
                    username,
                    _options.Username,
                    StringComparison.Ordinal) &&
                string.Equals(
                    password,
                    _options.Password,
                    StringComparison.Ordinal);
        }
        catch (FormatException)
        {
            return false;
        }
    }
}