namespace Kafka.Webhooks.Agent
{
    public class BaseConsumerSettings
    {
        public int RetryThreshold { get; set; }

        public int RetryBackoffMilliseconds { get; set; }

        public bool SkipRetriedMessagesEnabled { get; set; } = false;

        public int QueueBufferingMaxMessages { get; set; } = 1;

        public int PollIntervalMs { get; set; } = 100;
    }
}
