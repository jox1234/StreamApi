using Microsoft.AspNetCore.Mvc;
using StreamApi.Api.Models;
using StreamApi.Api.Services;

namespace StreamApi.Api.Controllers;

[ApiController]
[Route("api/v1/[controller]")]
public sealed class HealthController : ControllerBase
{
    private readonly IHealthService _healthService;

    public HealthController(IHealthService healthService)
    {
        _healthService = healthService;
    }

    [HttpGet]
    public ActionResult<HealthResponse> GetHealth()
    {
        return Ok(_healthService.GetHealth());
    }
}