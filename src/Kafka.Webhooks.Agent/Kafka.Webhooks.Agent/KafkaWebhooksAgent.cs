using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class KafkaWebhooksAgent : BaseConsumer<string>, IMicroservice
    {
        public KafkaWebhooksAgent(
            ILogger logger, 
            BaseConsumerSettings consumerSettings,
            KafkaWebhooksAgentSettings agentSettings,
            CancellationToken cancellationToken, 
            Consumer<string, string> confluentKafkaConsumer) 
            : base(logger, consumerSettings, cancellationToken, confluentKafkaConsumer)
        {
            AgentSettings = agentSettings;
        }

        public KafkaWebhooksAgentSettings AgentSettings { get; }

        public DateTime LastProcessedMessageDate { get; set; }

        public void Start()
        {
            RegisterEventHandlers();
            Subscribe(AgentSettings.Topics);
            StartPolling();
        }

        public Task<HealthStatusResponse> CheckHealthAsync()
        {
            var message = $"{nameof(LastProcessedMessageDate)} - {LastProcessedMessageDate:G}";
            var healthStatus = new HealthStatusResponse(HealthStatusType.Ok, message);

            return Task.FromResult(healthStatus);
        }

        protected override bool TryProcess(IEnumerable<Message<string, string>> messages)
        {
            // Send message to a list of webhook endpoints.

            LastProcessedMessageDate = DateTime.UtcNow;

            return true;
        }
    }
}
