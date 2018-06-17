using System;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public interface IExceptionListener : IDisposable
    {
        void Listen();

        ILogger Logger { get; } 
    }
}
