using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Kafka.Webhooks.Agent
{
    public class ConfluentConsumerSettings
    {
        [JsonProperty("group.id")]
        public string ConsumerGroup { get; set; }

        [JsonProperty("enable.auto.commit")]
        public bool EnableAutoCommit { get; set; }

        [JsonProperty("auto.commit.interval.ms")]
        public int AutoCommitIntervalMs { get; set; }

        [JsonProperty("statistics.interval.ms")]
        public int StatisticsIntervalMs { get; set; }

        [JsonProperty("socket.blocking.max.ms")]
        public int SocketBlockingMaxMs { get; set; }

        [JsonProperty("bootstrap.servers")]
        public List<string> BootstrapServers { get; set; }

        [JsonProperty("auto.offset.reset")]
        public string AutoOffsetReset { get; set; }

        public Dictionary<string, object> AsDictionary()
        {
            return new Dictionary<string, object>
            {
                { "group.id", ConsumerGroup },
                { "bootstrap.servers", string.Join(",", BootstrapServers) },
                { "enable.auto.commit", EnableAutoCommit },
                { "statistics.interval.ms", StatisticsIntervalMs },
                { "auto.commit.interval.ms", AutoCommitIntervalMs },
                { "socket.blocking.max.ms", SocketBlockingMaxMs },
                {
                    "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", AutoOffsetReset }
                    }
                }
            };
        }
    }
}
