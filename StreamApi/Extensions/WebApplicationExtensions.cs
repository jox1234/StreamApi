using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;

namespace StreamApi.Extensions;

public static class WebApplicationExtensions
{
    public static WebApplication UseStreamApi(
        this WebApplication app)
    {
        if (app.Environment.IsDevelopment())
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        app.UseHttpsRedirection();

        app.UseAuthorization();

        app.MapControllers();

        return app;
    }
}