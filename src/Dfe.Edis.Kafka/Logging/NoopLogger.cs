using System;

namespace Dfe.Edis.Kafka.Logging
{
    public class NoopLogger<T> : IKafkaLogger<T>
    {
        public void Log(LogLevel level, string message)
        {
        }

        public void Log(LogLevel level, string message, string client, string facility)
        {
        }
    }
}