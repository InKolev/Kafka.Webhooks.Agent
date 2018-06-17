using System;
using System.Collections.Generic;
using Autofac;
using Autofac.Core;

namespace Kafka.Webhooks.Agent.Extensions
{
    public static class AutofacExtensions
    {
        public static T ResolveUnregistered<T>(
            this IComponentContext context, 
            Type serviceType, 
            IEnumerable<Parameter> parameters)
        {
            var scope = context.Resolve<ILifetimeScope>();
            using (var innerScope = scope.BeginLifetimeScope(b => b.RegisterType(serviceType)))
            {
                innerScope.ComponentRegistry.TryGetRegistration(
                    new TypedService(serviceType), 
                    out var reg);

                return (T)context.ResolveComponent(reg, parameters);
            }
        }
    }
}
