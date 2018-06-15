using System.Diagnostics;
using System.Threading.Tasks;

namespace Kafka.Webhooks.Agent
{
    public static class TaskExtensions
    {
        public static async Task<(double executionTimeInMs, T result)> Measure<T>(this Task<T> task)
        {
            var stopwatch = Stopwatch.StartNew();
            var result = await task;
            stopwatch.Stop();

            var executionTimeInMs = stopwatch.Elapsed.TotalMilliseconds;
            return (executionTimeInMs, result);
        }

        public static async Task<double> Measure(this Task task)
        {
            var stopwatch = Stopwatch.StartNew();
            await task;
            stopwatch.Stop();

            var executionTimeInMs = stopwatch.Elapsed.TotalMilliseconds;
            return executionTimeInMs;
        }
    }
}
