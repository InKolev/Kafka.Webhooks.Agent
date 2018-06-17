using System;
using System.Runtime.ExceptionServices;
using Monitoring.Serilog.Extensions;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class FirstChanceExceptionListener : IExceptionListener
    {
        public FirstChanceExceptionListener(ILogger logger)
        {
            Logger = logger;
        }

        public ILogger Logger { get; }

        public void Listen()
        {
            AppDomain.CurrentDomain.FirstChanceException += OnFirstChanceException;
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
                AppDomain.CurrentDomain.FirstChanceException -= OnFirstChanceException;
            }
        }

        private void OnFirstChanceException(
            object sender, 
            FirstChanceExceptionEventArgs eventArgs)
        {
            var exception = eventArgs.Exception;

            Logger.ErrorStructured(new
            {
                LogSource = nameof(OnFirstChanceException),
                LogData = exception,
                LogSender = sender.ToString()
            });
        }

    }
}
