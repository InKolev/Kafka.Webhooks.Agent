using System;
using System.Linq;
using System.Threading;
using Autofac;
using Autofac.Core;
using Kafka.Webhooks.Agent.Extensions;
using Newtonsoft.Json.Linq;
using Serilog;

namespace Kafka.Webhooks.Agent
{
    public class MicroserviceInitializer : IDisposable
    {
        public MicroserviceInitializer(CancellationToken cancellationToken)
        {
            CancellationToken = cancellationToken;
        }

        public ILogger Logger { get; set; }

        public IContainer Container { get; set; }

        public Type MicroserviceType { get; set; }

        public IMicroservice Microservice { get; set; }

        public Type MicroserviceConfiguratorType { get; set; }

        public IMicroserviceConfigurator MicroserviceConfigurator { get; set; }

        public JObject MicroserviceSettings { get; set; }

        public CancellationToken CancellationToken { get; }

        public CancellationTokenSource CancellationTokenSource { get; set; } = 
            new CancellationTokenSource();

        public IExceptionListener FirstChanceExceptionListener { get; set; }

        public IExceptionListener UnhandledExceptionListener { get; set; }

        public MicroserviceInitializer Initialize()
        {
            LoadMicroServiceType()
                .LoadMicroServiceSettings()
                .LoadSerilog()
                .LoadMicroServiceConfiguratorType()
                .InitializeMicroserviceConfigurator()
                .ConfigureDependencyInjectionContainer()
                .AttachExceptionListeners()
                .InitializeMicroservice();

            return this;
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
                Container?.Dispose();
                Microservice?.Dispose();
                FirstChanceExceptionListener?.Dispose();
                UnhandledExceptionListener?.Dispose();
            }
        }

        private MicroserviceInitializer InitializeMicroservice()
        {
            Microservice = Container.ResolveUnregistered<IMicroservice>(
                MicroserviceType, 
                Enumerable.Empty<Parameter>());

            return this;
        }

        private MicroserviceInitializer InitializeMicroserviceConfigurator()
        {
            MicroserviceConfigurator = 
                Activator.CreateInstance(MicroserviceConfiguratorType) as IMicroserviceConfigurator;

            return this;
        }

        private MicroserviceInitializer LoadMicroServiceConfiguratorType()
        {
            MicroserviceConfiguratorType = AppDomain.CurrentDomain.GeSingleTypeByImplementedInterface<IMicroserviceConfigurator>();

            return this;
        }


        private MicroserviceInitializer LoadMicroServiceSettings()
        {
            MicroserviceSettings = new MicroserviceSettingsLoader().Load();

            return this;
        }

        private MicroserviceInitializer LoadSerilog()
        {
            Logger = new SerilogLoader().Load(MicroserviceType.Name);

            return this;
        }

        private MicroserviceInitializer LoadMicroServiceType()
        {
            MicroserviceType = AppDomain.CurrentDomain
                .GeSingleTypeByImplementedInterface<IMicroservice>();

            return this;
        }

        private MicroserviceInitializer AttachExceptionListeners()
        {
            FirstChanceExceptionListener = new FirstChanceExceptionListener(Logger);
            FirstChanceExceptionListener.Listen();

            UnhandledExceptionListener = new UnhandledExceptionListener(Logger);
            UnhandledExceptionListener.Listen();

            return this;
        }

        private MicroserviceInitializer ConfigureDependencyInjectionContainer()
        {
            var containerBuilder = new ContainerBuilder();

            containerBuilder.Register(c => CancellationToken)
                .As<CancellationToken>();

            containerBuilder.RegisterInstance(Logger)
                .As<ILogger>();

            MicroserviceConfigurator.Configure(containerBuilder, MicroserviceSettings);

            Container = containerBuilder.Build();

            return this;
        }
    }
}
