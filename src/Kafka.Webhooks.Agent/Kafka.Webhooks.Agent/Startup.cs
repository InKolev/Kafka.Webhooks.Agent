using System;
using System.Threading;
using System.Threading.Tasks;
using Monitoring.Serilog.Extensions;

namespace Kafka.Webhooks.Agent
{
    public class Startup
    {
        public static async Task Main(string[] args)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var microserviceInitializer = new MicroserviceInitializer(cancellationToken);
            microserviceInitializer.Initialize();

            var microservice = microserviceInitializer.Microservice;
            var logger = microserviceInitializer.Logger;

            var healthStatusReportService = new HealthStatusReportService(logger, microservice, cancellationToken);
            var reportInterval = TimeSpan.FromMilliseconds(30 * 1000);

            try
            {
                healthStatusReportService.StartReportingAsync(reportInterval);
                microservice.Start();
            }
            catch (Exception exception)
            {
                logger.ErrorStructured(new
                {
                    LogSource = nameof(Main),
                    LogData = exception
                });
            }
            finally
            {
                cancellationTokenSource.Cancel();
                microserviceInitializer.Dispose();
            }
        }
    }
}
