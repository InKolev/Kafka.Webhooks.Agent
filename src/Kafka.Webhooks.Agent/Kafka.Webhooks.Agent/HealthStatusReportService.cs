using System;
using System.Threading;
using System.Threading.Tasks;
using Monitoring.Serilog.Extensions;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class HealthStatusReportService
    {
        public HealthStatusReportService(
            ILogger logger,
            IMicroservice microservice,
            CancellationToken cancellationToken)
        {
            Logger = logger;
            Microservice = microservice;
            CancellationToken = cancellationToken;
        }

        private ILogger Logger { get; }

        private IMicroservice Microservice { get; }

        private CancellationToken CancellationToken { get; }

        public async Task StartReportingAsync(TimeSpan reportInterval)
        {
            while (!CancellationToken.IsCancellationRequested)
            {
                try
                {
                    var healthCheckResponse = await Microservice.CheckHealthAsync();
                    Logger.InformationStructured(healthCheckResponse);
                }
                catch (Exception exception)
                {
                    Logger.Error(exception);
                }

                await Task.Delay(reportInterval, CancellationToken);
            }
        }
    }
}
