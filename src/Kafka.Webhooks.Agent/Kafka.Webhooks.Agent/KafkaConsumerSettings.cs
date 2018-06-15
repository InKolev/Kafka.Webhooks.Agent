﻿namespace Kafka.Webhooks.Agent
{
    public class KafkaConsumerSettings
    {
        public int RetryThreshold { get; set; }

        public int RetryBackoffMilliseconds { get; set; }

        public bool SkipRetriedMessagesEnabled { get; set; } = false;

        public int QueueBufferingMaxMessages { get; set; } = 1;

        public int PollInterval { get; set; } = 100;
    }
}
