using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Logging
{
    internal static class KafkaLoggerExtensions
    {
        internal static void Log<T>(this IKafkaLogger<T> logger, LogMessage logMessage)
        {
            var level = (LogLevel) ((int) logMessage.Level);
            logger.Log(level, logMessage.Message, logMessage.Name, logMessage.Facility);
        }
    }
}