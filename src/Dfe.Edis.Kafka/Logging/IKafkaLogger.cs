namespace Dfe.Edis.Kafka.Logging
{
    public interface IKafkaLogger
    {
        void Log(LogLevel level, string message);
        void Log(LogLevel level, string message, string client, string facility);
    }
    public interface IKafkaLogger<T> : IKafkaLogger
    {
    }
}