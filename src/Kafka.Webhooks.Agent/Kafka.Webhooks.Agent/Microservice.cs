using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class Microservice : BaseConsumer<byte[]>, IMicroservice
    {
        public Microservice(
            ILogger logger, 
            BaseConsumerSettings settings, 
            CancellationToken cancellationToken, 
            Consumer<string, byte[]> confluentKafkaConsumer) 
            : base(logger, settings, cancellationToken, confluentKafkaConsumer)
        {
        }

        public void Start()
        {
            RegisterEventHandlers();

            Subscribe();
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }

        public Task<HealthStatusResponse> CheckHealthAsync()
        {
            throw new NotImplementedException();
        }
    }
}
