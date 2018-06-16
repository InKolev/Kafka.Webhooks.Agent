using System.Text;
using Autofac;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json.Linq;

namespace Kafka.Webhooks.Agent
{
    public class MicroserviceConfigurator : IMicroserviceConfigurator
    {
        public ContainerBuilder Configure(
            ContainerBuilder containerBuilder, 
            JObject serviceConfig)
        {
            var microserviceSettings = serviceConfig.ToObject<MicroserviceSettings>();

            RegisterBaseConsumerSettings(
                containerBuilder, 
                microserviceSettings.BaseConsumerSettings);

            RegisterConfluentKafkaConsumer(
                containerBuilder, 
                microserviceSettings.ConfluentConsumerSettings);

            return containerBuilder;
        }

        private static void RegisterBaseConsumerSettings(
            ContainerBuilder containerBuilder,
            BaseConsumerSettings settings)
        {
            containerBuilder.RegisterInstance(settings)
                .As<BaseConsumerSettings>();
        }

        private static void RegisterConfluentKafkaConsumer(
            ContainerBuilder containerBuilder,
            ConfluentConsumerSettings settings)
        {
            var consumerConfig = settings.AsDictionary();
            var keyDeserializer = new StringDeserializer(Encoding.UTF8);
            var valueDeserializer = new JsonDeserializer<string>();

            var consumer = new Consumer<string, string>(
                consumerConfig, keyDeserializer, valueDeserializer);

            containerBuilder.RegisterInstance(consumer)
                .As<Consumer<string, string>>();
        }
    }
}
