using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace Kafka.Webhooks.Agent
{
    public class JsonDeserializer<T> : IDeserializer<T>
    {
        public JsonDeserializer()
        {
            SerializerSettings = new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.None
            };
        }

        public JsonDeserializer(JsonSerializerSettings serializerSettings)
        {
            SerializerSettings = serializerSettings;
        }

        protected JsonSerializerSettings SerializerSettings { get; set; }

        public T Deserialize(string topic, byte[] data)
        {
            return JsonConvert.DeserializeObject<T>(
                Encoding.UTF8.GetString(data), SerializerSettings);
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(
            IEnumerable<KeyValuePair<string, object>> config, 
            bool isKey)
        {
            return config;
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
                // Dispose resources here.
            }
        }
    }
}
