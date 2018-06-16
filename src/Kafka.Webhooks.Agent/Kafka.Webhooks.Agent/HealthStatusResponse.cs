namespace Kafka.Webhooks.Agent
{
    public class HealthStatusResponse
    {
        public HealthStatusResponse()
        {
        }

        public HealthStatusResponse(HealthStatusType status)
        {
            this.Status = status;
        }

        public HealthStatusResponse(HealthStatusType status, string message)
        {
            this.Status = status;
            this.Message = message;
        }

        public string Message { get; set; }

        public HealthStatusType Status { get; set; }

        public static HealthStatusResponse Ok => 
            new HealthStatusResponse(HealthStatusType.Ok);

        public static HealthStatusResponse Warning => 
            new HealthStatusResponse(HealthStatusType.Warning);

        public static HealthStatusResponse Critical => 
            new HealthStatusResponse(HealthStatusType.Critical);
    }
}
