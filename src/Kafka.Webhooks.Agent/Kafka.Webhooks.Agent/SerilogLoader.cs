using Microsoft.Extensions.Configuration;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class SerilogLoader
    {
        public ILogger Load(string applicationName)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("settings.serilog.json")
                .Build();

            var logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .Enrich.WithProperty("Application", applicationName)
                .Enrich.WithMachineName()
                .CreateLogger();

            // Set the globally shared logger to be this Serilog instance.
            Log.Logger = logger;

            return logger;
        }

    }
}
