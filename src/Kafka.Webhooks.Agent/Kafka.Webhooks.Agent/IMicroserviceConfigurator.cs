using Autofac;
using Newtonsoft.Json.Linq;

namespace Kafka.Webhooks.Agent
{
    public interface IMicroserviceConfigurator
    {
        ContainerBuilder Configure(ContainerBuilder containerBuilder, JObject serviceConfig);
    }
}
