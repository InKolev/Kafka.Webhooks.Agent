using System.Collections.Generic;

namespace Kafka.Webhooks.Agent
{
    public class KafkaWebhooksAgentSettings
    {
        public IEnumerable<string> Topics { get; set; }
    }
}
