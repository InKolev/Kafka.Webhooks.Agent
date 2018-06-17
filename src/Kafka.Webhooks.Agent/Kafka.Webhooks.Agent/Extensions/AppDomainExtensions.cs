using System;
using System.Linq;

namespace Kafka.Webhooks.Agent.Extensions
{
    public static class AppDomainExtensions
    {
        public static Type GeSingleTypeByImplementedInterface<T>(this AppDomain appDomain)
            where T : class
        {
            var interfaceType = typeof(T);

            var type = appDomain.GetAssemblies()
                .Where(x => !x.IsDynamic)
                .SelectMany(x => x.GetExportedTypes())
                .SingleOrDefault(
                    x =>
                        interfaceType.IsAssignableFrom(x)
                        && x.IsClass
                        && !x.IsAbstract
                        && !x.IsInterface);

            if (type == null)
            {
                throw new TypeLoadException(
                    $"Could not find an implementation of {interfaceType.FullName} in any of the loaded assemblies.");
            }

            return type;
        }
    }
}
