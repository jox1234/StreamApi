using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace StreamApi.Api.TargetApi;

public interface ITargetApiClient
{
    Task<TargetApiClientResult> PostAsync(
        IReadOnlyList<TargetApiData> data,
        CancellationToken cancellationToken);
}

public sealed class TargetApiClient : ITargetApiClient
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<TargetApiClient> _logger;
    private readonly TargetApiOptions _options;

    private readonly SemaphoreSlim _tokenLock = new(1, 1);

    private string? _jwtToken;

    public TargetApiClient(
        HttpClient httpClient,
        IOptions<TargetApiOptions> options,
        ILogger<TargetApiClient> logger)
    {
        _httpClient = httpClient;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<TargetApiClientResult> PostAsync(
        IReadOnlyList<TargetApiData> data,
        CancellationToken cancellationToken)
    {
        if (data is null)
            throw new ArgumentNullException(nameof(data));

        if (data.Count == 0)
            throw new ArgumentException(
                "Target API data cannot be empty.",
                nameof(data));

        var token =
            await GetTokenAsync(cancellationToken);

        var result =
            await SendPostAsync(
                data,
                token,
                cancellationToken);

        if (result.StatusCode == (int)HttpStatusCode.Unauthorized)
        {
            _logger.LogWarning(
                "[TARGET_API_JWT_EXPIRED] Target API returned 401. " +
                "Refreshing JWT and retrying once.");

            await InvalidateTokenAsync();

            token =
                await GetTokenAsync(cancellationToken);

            result =
                await SendPostAsync(
                    data,
                    token,
                    cancellationToken);
        }

        return result;
    }

    private async Task<string> GetTokenAsync(
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_jwtToken))
            return _jwtToken;

        await _tokenLock.WaitAsync(cancellationToken);

        try
        {
            if (!string.IsNullOrWhiteSpace(_jwtToken))
                return _jwtToken;

            _logger.LogInformation(
                "[TARGET_API_LOGIN_START] " +
                "Requesting JWT from Target API.");

            var loginPayload = new
            {
                userName = _options.Username,
                password = _options.Password
            };

            using var response =
                await _httpClient.PostAsJsonAsync(
                    _options.LoginPath,
                    loginPayload,
                    cancellationToken);

            var responseBody =
                await response.Content.ReadAsStringAsync(
                    cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError(
                    "[TARGET_API_LOGIN_FAILED] " +
                    "statusCode={StatusCode} response={Response}",
                    (int)response.StatusCode,
                    responseBody);

                throw new HttpRequestException(
                    $"Target API login failed. " +
                    $"HTTP {(int)response.StatusCode}.");
            }

            var loginResponse =
                JsonSerializer.Deserialize<LoginResponse>(
                    responseBody,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

            if (loginResponse is null ||
                string.IsNullOrWhiteSpace(loginResponse.Token))
            {
                _logger.LogError(
                    "[TARGET_API_LOGIN_INVALID_RESPONSE] " +
                    "Target API login response does not contain a JWT token.");

                throw new InvalidOperationException(
                    "Target API login response does not contain a JWT token.");
            }

            _jwtToken = loginResponse.Token;

            _logger.LogInformation(
                "[TARGET_API_LOGIN_SUCCESS] JWT acquired successfully.");

            return _jwtToken;
        }
        finally
        {
            _tokenLock.Release();
        }
    }

    private async Task<TargetApiClientResult> SendPostAsync(
        IReadOnlyList<TargetApiData> data,
        string token,
        CancellationToken cancellationToken)
    {
        using var request =
            new HttpRequestMessage(
                HttpMethod.Post,
                _options.TargetPath);

        request.Headers.Authorization =
            new AuthenticationHeaderValue(
                "Bearer",
                token);

        request.Content =
            JsonContent.Create(data);

        _logger.LogInformation(
            "[TARGET_API_POST_START] " +
            "endpoint={Endpoint} rows={RowCount}",
            _options.TargetPath,
            data.Count);

        using var response =
            await _httpClient.SendAsync(
                request,
                cancellationToken);

        var responseBody =
            await response.Content.ReadAsStringAsync(
                cancellationToken);

        _logger.LogInformation(
            "[TARGET_API_POST_RESULT] " +
            "statusCode={StatusCode} response={Response}",
            (int)response.StatusCode,
            responseBody);

        return new TargetApiClientResult(
            response.IsSuccessStatusCode,
            (int)response.StatusCode,
            responseBody);
    }

    private Task InvalidateTokenAsync()
    {
        _jwtToken = null;

        return Task.CompletedTask;
    }

    private sealed class LoginResponse
    {
        public string Token { get; set; } = string.Empty;

        public string? ExpiresAt { get; set; }
    }
}

public sealed record TargetApiClientResult(
    bool IsSuccessful,
    int StatusCode,
    string ResponseBody);