using Microsoft.AspNetCore.Mvc;
using StreamApi.Api.TargetApi;

namespace StreamApi.Api.Controllers;

[ApiController]
[Route("TargetApi")]
public sealed class TargetApiController : ControllerBase
{
    private readonly ILogger<TargetApiController> _logger;

    public TargetApiController(
        ILogger<TargetApiController> logger)
    {
        _logger = logger;
    }

    [HttpPost]
    public IActionResult Post(
        [FromBody] IReadOnlyList<TargetApiData> data)
    {
        if (data is null)
        {
            return BadRequest(new
            {
                message = "Request body is required."
            });
        }

        if (data.Count == 0)
        {
            return BadRequest(new
            {
                message = "Request body must contain at least one item."
            });
        }

        for (var i = 0; i < data.Count; i++)
        {
            var item = data[i];

            if (string.IsNullOrWhiteSpace(
                    item.KodeToko))
            {
                return BadRequest(new
                {
                    message =
                        $"kodeToko is required at index {i}."
                });
            }

            if (string.IsNullOrWhiteSpace(
                    item.Table))
            {
                return BadRequest(new
                {
                    message =
                        $"table is required at index {i}."
                });
            }
        }

        _logger.LogInformation(
            "[TARGET_API_POST_RECEIVED] " +
            "rows={RowCount}",
            data.Count);

        foreach (var item in data)
        {
            _logger.LogInformation(
                "[TARGET_API_POST_ITEM] " +
                "kodeToko={KodeToko} " +
                "table={Table} " +
                "dataServer={DataServer} " +
                "tanggalProses={TanggalProses} " +
                "tanggalDT3={TanggalDT3}",
                item.KodeToko,
                item.Table,
                item.DataServer,
                item.TanggalProses,
                item.TanggalDT3);
        }

        return Ok(new
        {
            message = "Data received successfully.",
            rowCount = data.Count,
            data
        });
    }
}