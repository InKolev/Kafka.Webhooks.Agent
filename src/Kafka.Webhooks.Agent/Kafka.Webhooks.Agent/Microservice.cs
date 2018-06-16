using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class Microservice : KafkaConsumer<byte[]>, IMicroservice
    {
        public Microservice(
            ILogger logger, 
            KafkaConsumerSettings settings, 
            CancellationToken cancellationToken, 
            Consumer<string, byte[]> confluentKafkaConsumer) 
            : base(logger, settings, cancellationToken, confluentKafkaConsumer)
        {
        }

        public void Start()
        {
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
