using System.IO;
using System.Reflection;
using Newtonsoft.Json.Linq;

namespace Kafka.Webhooks.Agent
{
    public class MicroserviceSettingsLoader
    {
        public JObject Load()
        {
            var assemblyLocation = Directory.GetParent(Assembly.GetCallingAssembly().Location);
            var settingsFilePath = $@"{assemblyLocation}/settings.microservice.json";

            if (!File.Exists(settingsFilePath))
            {
                throw new FileNotFoundException(
                    $"Failed to load settings file. Check if {settingsFilePath} exists inside the project build directory.");
            }

            return JObject.Parse(
                File.ReadAllText(settingsFilePath));
        }
    }
}
