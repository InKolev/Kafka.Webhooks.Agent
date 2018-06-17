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
    public abstract class BaseConsumer<TMessage> : IDisposable
       where TMessage : class
    {
        protected BaseConsumer(
            ILogger logger,
            BaseConsumerSettings consumerSettings,
            CancellationToken cancellationToken,
            Consumer<string, TMessage> confluentKafkaConsumer)
        {
            Logger = logger;
            ConsumerSettings = consumerSettings;
            CancellationToken = cancellationToken;
            ConfluentKafkaConsumer = confluentKafkaConsumer;

            var capacity = consumerSettings.QueueBufferingMaxMessages + 1;
            MessagesQueue =
                new Queue<Message<string, TMessage>>(capacity);

            LastReceivedMessagePerTopicPartition =
                new Dictionary<TopicPartition, Message<string, TMessage>>();
        }

        protected ILogger Logger { get; set; }

        protected Consumer<string, TMessage> ConfluentKafkaConsumer { get; set; }

        protected BaseConsumerSettings ConsumerSettings { get; }

        protected Queue<Message<string, TMessage>> MessagesQueue { get; set; }

        protected Dictionary<TopicPartition, Message<string, TMessage>> LastReceivedMessagePerTopicPartition { get; set; }

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
            ConfluentKafkaConsumer.OnMessage += OnMessage;
        }

        protected virtual void OnMessage(
            object sender,
            Message<string, TMessage> message)
        {
            MessagesQueue.Enqueue(message);
            SetLastReceivedMessagePerTopicPartition(message);
            if (MessagesQueue.Count >= ConsumerSettings.QueueBufferingMaxMessages)
            {
                ProcessQueue();
            }
        }

        private void ProcessQueue()
        {
            var retryCount = 0;
            var isProcessed = false;
            while (!isProcessed)
            {
                try
                {
                    ++retryCount;
                    if (ShouldSkipRetriedMessages(retryCount))
                    {
                        LogSkippedMessages();
                        break;
                    }

                    isProcessed = TryProcess(MessagesQueue);

                    if (isProcessed)
                    {
                        LogProcessedMessages();
                    }
                    else
                    {
                        LogRetriedMessages();
                        BackOff();
                    }
                }
                catch (Exception ex)
                {
                    LogException(ex);
                    BackOff();
                }
            }

            CommitLastReceivedMessagePerTopicPartition();
            MessagesQueue.Clear();
            LastReceivedMessagePerTopicPartition.Clear();
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
            Logger.InformationStructured(new
            {
                LogSource = nameof(StartPolling),
                LogMessage = "Started polling for messages."
            });

            while (!CancellationToken.IsCancellationRequested)
            {
                ConfluentKafkaConsumer.Poll(
                    TimeSpan.FromMilliseconds(ConsumerSettings.PollIntervalMs));
            }

            Logger.InformationStructured(new
            {
                LogSource = nameof(StartPolling),
                LogMessage = "Polling stopped gracefully."
            });
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
                LogMessage = $"Backing off for {ConsumerSettings.RetryBackoffMilliseconds} ms."
            });

            Task.Delay(ConsumerSettings.RetryBackoffMilliseconds, CancellationToken)
                .Wait(CancellationToken);
        }

        protected abstract bool TryProcess(
            IEnumerable<Message<string, TMessage>> messages);

        private void SetLastReceivedMessagePerTopicPartition(
            Message<string, TMessage> message)
        {
            var topicPartition = message.TopicPartition;
            if (!LastReceivedMessagePerTopicPartition.ContainsKey(topicPartition))
            {
                LastReceivedMessagePerTopicPartition.Add(topicPartition, message);
            }
            else
            {
                var cachedMessage = LastReceivedMessagePerTopicPartition[topicPartition];
                if (message.Offset.Value > cachedMessage.Offset.Value)
                {
                    LastReceivedMessagePerTopicPartition[topicPartition] = message;
                }
            }
        }

        private void CommitLastReceivedMessagePerTopicPartition()
        {
            foreach (var keyValuePair in LastReceivedMessagePerTopicPartition)
            {
                // TODO: Think if you want to await this or not. What are the guarantees in both cases.
                CommitAsync(keyValuePair.Value);
            }
        }

        private bool ShouldSkipRetriedMessages(int retryCount)
        {
            if (ConsumerSettings.SkipRetriedMessagesEnabled)
            {
                return retryCount >= ConsumerSettings.RetryThreshold;
            }

            return false;
        }

        private void LogProcessedMessages()
        {
            Logger.InformationStructured(new
            {
                LogSource = nameof(LogProcessedMessages),
                LogMessage = "Successfully processed messages batch.",
                LogData = MessagesQueue.Select(x => new
                {
                    x.Topic,
                    x.Partition,
                    x.Offset
                })
            });
        }

        private void LogRetriedMessages()
        {
            Logger.WarningStructured(new
            {
                LogSource = nameof(LogRetriedMessages),
                LogMessage = "Retrying messages batch.",
                LogData = MessagesQueue.Select(x => new
                {
                    x.Topic,
                    x.Partition,
                    x.Offset
                })
            });
        }

        private void LogSkippedMessages()
        {
            var logMessage =
                $"Skipping messages batch because {nameof(ConsumerSettings.RetryThreshold)} " +
                $"of {ConsumerSettings.RetryThreshold} was exceeded.";

            Logger.WarningStructured(new
            {
                LogSource = nameof(LogSkippedMessages),
                LogMessage = logMessage,
                LogData = MessagesQueue.Select(x => new
                {
                    x.Topic,
                    x.Partition,
                    x.Offset
                })
            });
        }

        private void LogException(Exception exc)
        {
            Logger.ErrorStructured(new
            {
                LogSource = nameof(LogException),
                LogMessage = exc.ToString(),
                LogData = MessagesQueue.Select(x => new
                {
                    x.Topic,
                    x.Partition,
                    x.Offset
                })
            });
        }
    }
}
