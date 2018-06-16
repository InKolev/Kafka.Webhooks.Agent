namespace Kafka.Webhooks.Agent
{
    public class MicroserviceSettings
    {
        public BaseConsumerSettings BaseConsumerSettings { get; set; }

        public ConfluentConsumerSettings ConfluentConsumerSettings { get; set; }
    }
}
