using System;
using System.Threading.Tasks;

namespace Kafka.Webhooks.Agent
{
    public interface IMicroservice : IDisposable
    {
        void Start();

        Task<HealthStatusResponse> CheckHealthAsync();
    }
}
