namespace Dfe.Edis.Kafka.Logging
{
    public interface IKafkaLogger<T>
    {
        void Log(LogLevel level, string message);
        void Log(LogLevel level, string message, string client, string facility);
    }
}