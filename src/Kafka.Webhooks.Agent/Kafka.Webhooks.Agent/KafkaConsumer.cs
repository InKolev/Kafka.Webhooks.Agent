using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Monitoring.Serilog.Extensions;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public abstract class KafkaConsumer<TMessage> : IDisposable
       where TMessage : class
    {
        protected KafkaConsumer(
            ILogger logger,
            KafkaConsumerSettings settings,
            CancellationToken cancellationToken,
            Consumer<string, TMessage> confluentKafkaConsumer)
        {
            Logger = logger;
            Settings = settings;
            CancellationToken = cancellationToken;
            ConfluentKafkaConsumer = confluentKafkaConsumer;
        }

        protected ILogger Logger { get; set; }

        protected Consumer<string, TMessage> ConfluentKafkaConsumer { get; set; }

        protected KafkaConsumerSettings Settings { get; }

        protected CancellationToken CancellationToken { get; }

        public virtual void RegisterEventHandlers()
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(RegisterEventHandlers),
                LogMessage = "Registering event handlers.",
            });

            ConfluentKafkaConsumer.OnError += OnError;
            ConfluentKafkaConsumer.OnConsumeError += OnConsumeError;
            ConfluentKafkaConsumer.OnPartitionsRevoked += OnPartitionsRevoked;
            ConfluentKafkaConsumer.OnPartitionsAssigned += OnPartitionsAssigned;
            ConfluentKafkaConsumer.OnLog += OnLog;
            ConfluentKafkaConsumer.OnPartitionEOF += OnPartitionEof;
            ConfluentKafkaConsumer.OnStatistics += OnStatistics;
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(Subscribe),
                LogData = topics,
                LogMessage = "Subscribing to topics."
            });

            ConfluentKafkaConsumer.Subscribe(topics);
        }

        public async Task<CommittedOffsets> CommitAsync(
            Message<string, TMessage> message)
        {
            var (executionTimeMs, commitReport) = 
                await ConfluentKafkaConsumer.CommitAsync(message).Measure();

            Logger.InformationStructured(new
            {
                LogSource = nameof(CommitAsync),
                LogData = commitReport,
                LogMessage = $"CommitAsync execution time: {executionTimeMs} ms.",
            });

            return commitReport;
        }

        public async Task<CommittedOffsets> CommitAsync(
            IEnumerable<TopicPartitionOffset> topicPartitionOffsets)
        {
            var (executionTimeMs, commitReport) =
                await ConfluentKafkaConsumer.CommitAsync(topicPartitionOffsets).Measure();

            Logger.InformationStructured(new
            {
                LogSource = nameof(CommitAsync),
                LogData = commitReport,
                LogMessage = $"CommitAsync execution time: {executionTimeMs} ms.",
            });

            return commitReport;
        }

        public void StartPolling()
        {
            Logger.Information("Started polling for messages.");

            while (!CancellationToken.IsCancellationRequested)
            {
                ConfluentKafkaConsumer.Poll(
                    TimeSpan.FromMilliseconds(Settings.PollIntervalMs));
            }

            Logger.Information("Listening stopped gracefully.");
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Logger.InformationStructured(new
                {
                    LogSource = nameof(Dispose),
                    LogMessage = $"Disposing {nameof(ConfluentKafkaConsumer)}."
                });

                ConfluentKafkaConsumer?.Dispose();

                Logger.InformationStructured(new
                {
                    LogSource = nameof(Dispose),
                    LogMessage = $"{nameof(ConfluentKafkaConsumer)} successfully disposed."
                });
            }
        }

        private void OnStatistics(
            object sender, 
            string statistics)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnStatistics),
                LogData = statistics,
                LogSender = sender.ToString()
            });
        }

        protected virtual void OnPartitionEof(
            object sender, 
            TopicPartitionOffset topicPartitionOffset)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnPartitionEof),
                LogData = topicPartitionOffset,
                LogSender = sender.ToString()
            });
        }

        protected virtual void OnLog(
            object sender, 
            LogMessage logMessage)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnLog),
                LogData = logMessage,
                LogSender = sender.ToString()
            });
        }

        protected virtual void OnConsumeError(
            object sender,
            Message message)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnConsumeError),
                LogData = message,
                LogSender = sender.ToString(),
            });
        }

        protected virtual void OnError(
            object sender,
            Error error)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnError),
                LogData = error,
                LogSender = sender.ToString(),
            });
        }

        protected virtual void OnPartitionsRevoked(
            object sender, 
            List<TopicPartition> revokedTopicPartitions)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnPartitionsRevoked),
                LogData = revokedTopicPartitions,
                LogSender = sender.ToString(),
                LogMessage = "Revoking topic partitions."
            });

            ConfluentKafkaConsumer.Unassign();

            Logger.InformationStructured(new
            {
                LogSource = nameof(OnPartitionsRevoked),
                LogData = revokedTopicPartitions,
                LogSender = sender.ToString(),
                LogMessage = "Topic partitions revoked successfully."
            });
        }

        protected virtual void OnPartitionsAssigned(
            object sender, 
            List<TopicPartition> topicPartitionsToAssign)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnPartitionsAssigned),
                LogData = topicPartitionsToAssign,
                LogSender = sender.ToString(),
                LogMessage = "Assigning topic partitions."
            });

            ConfluentKafkaConsumer.Assign(topicPartitionsToAssign);

            Logger.InformationStructured(new
            {
                LogSource = nameof(OnPartitionsAssigned),
                LogData = topicPartitionsToAssign,
                LogSender = sender.ToString(),
                LogMessage = "Topic partitions assigned successfully."
            });
        }
        
        protected void BackOff()
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(BackOff),
                LogMessage = $"Backing off for {Settings.RetryBackoffMilliseconds} ms."
            });

            Task.Delay(Settings.RetryBackoffMilliseconds, CancellationToken)
                .Wait(CancellationToken);
        }
    }
}
