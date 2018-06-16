using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace Kafka.Webhooks.Agent
{
    public class JsonSerializer<T> : ISerializer<T>
    {
        public JsonSerializer()
        {
            SerializerSettings = new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.None
            };
        }

        public JsonSerializer(JsonSerializerSettings serializerSettings)
        {
            SerializerSettings = serializerSettings;
        }

        protected JsonSerializerSettings SerializerSettings { get; set; }

        public byte[] Serialize(string topic, T data)
        {
            return Encoding.UTF8.GetBytes(
                JsonConvert.SerializeObject(data, SerializerSettings));
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
