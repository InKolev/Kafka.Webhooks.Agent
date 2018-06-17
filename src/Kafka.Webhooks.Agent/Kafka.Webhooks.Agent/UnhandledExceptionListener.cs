using System;
using Monitoring.Serilog.Extensions;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class UnhandledExceptionListener : IExceptionListener
    {
        public UnhandledExceptionListener(ILogger logger)
        {
            Logger = logger;
        }

        public ILogger Logger { get; } 

        public void Listen()
        {
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
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
                AppDomain.CurrentDomain.UnhandledException -= OnUnhandledException;
            }
        }

        private void OnUnhandledException(
            object sender, 
            UnhandledExceptionEventArgs e)
        {
            var exception = (Exception)e.ExceptionObject;

            Logger.ErrorStructured(new
            {
                LogSource = nameof(OnUnhandledException),
                LogData = exception,
                LogSender = sender.ToString()
            });
        }
    }
}
