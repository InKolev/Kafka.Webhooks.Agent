using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka.Webhooks.Agent
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            await Task.Delay(1000);
        }
    }
}
