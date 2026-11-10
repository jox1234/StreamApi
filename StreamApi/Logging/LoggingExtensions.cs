using Serilog;
using Serilog.Events;

namespace StreamApi.Logging;

public static class LoggingExtensions
{
    public static WebApplicationBuilder AddStreamApiLogging(
        this WebApplicationBuilder builder)
    {
        var logDirectory = Path.Combine(
            builder.Environment.ContentRootPath,
            "Logs");

        Directory.CreateDirectory(logDirectory);

        var logFile = Path.Combine(
            logDirectory,
            "Tracelog-.log");

        Log.Logger = new LoggerConfiguration()
                        .MinimumLevel.Information()

                        // Keep our application logs at Information.
                        .MinimumLevel.Override(
                            "StreamApi",
                            LogEventLevel.Information)

                        // Reduce ASP.NET Core framework noise.
                        .MinimumLevel.Override(
                            "Microsoft.AspNetCore",
                            LogEventLevel.Warning)

                        .MinimumLevel.Override(
                            "Microsoft.Hosting.Lifetime",
                            LogEventLevel.Information)

                        .WriteTo.Console(
                            outputTemplate:
                                "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext} {Message:lj}{NewLine}{Exception}")

                        .WriteTo.File(
                            path: logFile,
                            rollingInterval: RollingInterval.Day,
                            retainedFileCountLimit: 30,
                            shared: false,
                            flushToDiskInterval: TimeSpan.FromSeconds(1),
                            outputTemplate:
                                "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {SourceContext} {Message:lj}{NewLine}{Exception}")

                        .CreateLogger();

        builder.Host.UseSerilog();

        return builder;
    }
}