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
            IStatsCollector statsCollector,
            Consumer<string, TMessage> confluentConsumer,
            KafkaConsumerSettings settings,
            CancellationToken cancellationToken)
        {
            Logger = logger;
            StatsCollector = statsCollector;
            ConfluentConsumer = confluentConsumer;
            Settings = settings;
            CancellationToken = cancellationToken;
        }

        protected ILogger Logger { get; set; }

        protected IStatsCollector StatsCollector { get; }

        protected Consumer<string, TMessage> ConfluentConsumer { get; set; }

        protected KafkaConsumerSettings Settings { get; }

        protected CancellationToken CancellationToken { get; }

        public virtual void RegisterEventHandlers()
        {
            Logger.Information("Registering Base event handlers...");

            ConfluentConsumer.OnError += OnError;
            ConfluentConsumer.OnConsumeError += OnConsumeError;
            ConfluentConsumer.OnPartitionsRevoked += OnPartitionsRevoked;
            ConfluentConsumer.OnPartitionsAssigned += OnPartitionsAssigned;
            ConfluentConsumer.OnLog += OnLog;
            ConfluentConsumer.OnPartitionEOF += OnPartitionEof;
            ConfluentConsumer.OnStatistics += OnStatistics;

            Logger.Information("Base event handlers successfully registered.");
        }

        private void OnStatistics(object sender, string statistics)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnStatistics),
                LogData = statistics,
                LogSender = sender.ToString()
            });
        }

        private void OnPartitionEof(object sender, TopicPartitionOffset topicPartitionOffset)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnPartitionEof),
                LogData = topicPartitionOffset,
                LogSender = sender.ToString()
            });
        }

        private void OnLog(object sender, LogMessage logMessage)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(OnLog),
                LogData = logMessage,
                LogSender = sender.ToString()
            });
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(Subscribe),
                LogData = topics,
            });
            
            ConfluentConsumer.Subscribe(topics);
        }

        public async Task<CommittedOffsets> Commit(Message<string, TMessage> message)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(Commit),
                LogData = message.TopicPartitionOffset,
                LogMessage = "Committing offset."
            });

            var (x,a) =  await ConfluentConsumer.CommitAsync(message).Measure();

            Logger.InformationStructured(new
            {
                LogSource = nameof(Commit),
                LogData = commitReport,
                LogMessage = "Commit report."
            });

            return commitReport;
        }

        public async Task<CommittedOffsets> Commit(IEnumerable<TopicPartitionOffset> topicPartitionOffsets)
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(Commit),
                LogData = topicPartitionOffsets,
                LogMessage = "Committing offsets."
            });

            var commitReport = await ConfluentConsumer.CommitAsync(topicPartitionOffsets);

            Logger.InformationStructured(new
            {
                LogSource = nameof(Commit),
                LogData = commitReport,
                LogMessage = "Commit report."
            });

            Logger.Information("Committing offset for: {@TopicPartitionOffset}...", topicPartitionOffsets);

            //var (executionTime, result) = await ConfluentConsumer.CommitAsync(topicPartitionOffsets).Measure();

            var commitReport = await Stats.Time(
                async () => await ConfluentConsumer.CommitAsync(topicPartitionOffsets),
                $"{nameof(ConfluentConsumer.CommitAsync)}.ExecutionTime");

            Logger.Information("Commit report: {@CommitReport}.", commitReport);

            return commitReport;
        }

        public void StartPolling()
        {
            Logger.Information("Start listening for messages...");

            while (!CancellationToken.IsCancellationRequested)
            {
                Stats.Time(
                    () => ConfluentConsumer.Poll(TimeSpan.FromMilliseconds(Settings.PollInterval)),
                    $"{nameof(ConfluentConsumer.Poll)}.ExecutionTime");
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
                Logger.Information("Disposing BaseKafkaConsumer...");

                ConfluentConsumer?.Dispose();

                Logger.Information("Disposed BaseKafkaConsumer successfully.");
            }
        }

        protected void OnPartitionsRevoked(object sender, List<TopicPartition> e)
        {
            Logger.Information("Revoking partitions...");

            ConfluentConsumer.Unassign();

            Logger.Information("Partitions revoked successfully: {@Partitions}", e);
        }

        protected void OnPartitionsAssigned(object sender, List<TopicPartition> e)
        {
            Logger.Information("Assigning partitions...");

            ConfluentConsumer.Assign(e);

            Logger.Information("Partitions assigned: {@Partitions}", e);
        }

        protected void OnConsumeError(object sender, Message e)
        {
            Logger.Information("Consume error: {@Error}", e);
        }

        protected void OnError(object sender, Error e)
        {
            Logger.Information("General error: {@Error}", e);
        }

        /// <summary>
        /// Waiting a configurable amount of time before continuing execution 
        /// in hope that if the failure was result from network connectivity issues
        /// the system will have time to restore it's working state automatically.
        /// </summary>
        protected void BackOff()
        {
            Logger.Information(
                "Backing off for {@BackOffMilliseconds} ms.",
                Settings.BackOffOnTryProcessFailureInMilliseconds);

            Task.Delay(Settings.BackOffOnTryProcessFailureInMilliseconds, CancellationToken).Wait(CancellationToken);
        }
    }
}
