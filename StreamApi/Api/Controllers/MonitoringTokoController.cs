using Microsoft.AspNetCore.Mvc;
using StreamApi.Api.MonitoringToko;
using StreamApi.Processing.Inquiry;

namespace StreamApi.Api.Controllers;

[ApiController]
[Route("MonitoringToko")]
public sealed class MonitoringTokoController : ControllerBase
{
    private readonly MonitoringTokoService _service;
    private readonly MonitoringTokoBasicAuthentication _authentication;
    private readonly InquiryService _inquiryService;

    public MonitoringTokoController(
        MonitoringTokoService service,
        MonitoringTokoBasicAuthentication authentication,
        InquiryService inquiryService)
    {
        _service = service;
        _authentication = authentication;
        _inquiryService = inquiryService;
    }

    [HttpGet("inquiry")]
    public IActionResult GetInquiry()
    {
        if (!_authentication.IsValid(
                Request.Headers.Authorization))
        {
            Response.Headers.WWWAuthenticate = "Basic";

            return Unauthorized(new
            {
                message = "Invalid username or password."
            });
        }

        var rows =
            _inquiryService.GetSnapshot();

        return Ok(new
        {
            rowCount = rows.Count,
            data = rows
        });
    }

    [HttpGet("{tableName}")]
    public async Task<IActionResult> Get(
        string tableName,
        [FromQuery] string? storeCode,
        [FromQuery] string? branchCode,
        [FromQuery] long? tglProses,
        CancellationToken cancellationToken)
    {
        if (!_authentication.IsValid(
                Request.Headers.Authorization))
        {
            Response.Headers.WWWAuthenticate = "Basic";

            return Unauthorized(new
            {
                message = "Invalid username or password."
            });
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            return BadRequest(new
            {
                message = "tableName is required."
            });
        }

        if (string.IsNullOrWhiteSpace(storeCode))
        {
            return BadRequest(new
            {
                message = "storeCode is required."
            });
        }

        if (string.IsNullOrWhiteSpace(branchCode))
        {
            return BadRequest(new
            {
                message = "branchCode is required."
            });
        }

        if (!tglProses.HasValue)
        {
            return BadRequest(new
            {
                message = "tglProses is required."
            });
        }

        try
        {
            var rows =
                await _service.GetAsync(
                    tableName,
                    storeCode,
                    branchCode,
                    tglProses.Value,
                    cancellationToken);

            return Ok(new
            {
                tableName,
                storeCode,
                branchCode,
                tglProses,
                rowCount = rows.Count,
                data = rows
            });
        }
        catch (MonitoringTokoStoreNotFoundException ex)
        {
            return NotFound(new
            {
                message = ex.Message
            });
        }
        catch (MonitoringTokoBranchNotFoundException ex)
        {
            return NotFound(new
            {
                message = ex.Message
            });
        }
        catch (MonitoringTokoBranchMismatchException ex)
        {
            return BadRequest(new
            {
                message = ex.Message
            });
        }
    }
}