namespace Dfe.Edis.Kafka.Logging
{
    internal class KafkaLoggerWrapper<T> : IKafkaLogger<T>
    {
        private IKafkaLogger<T> _logger;

        public KafkaLoggerWrapper(KafkaLoggerFactory loggerFactory)
        {
            _logger = loggerFactory.GetLogger<T>();
        }

        public void Log(LogLevel level, string message)
        {
            _logger.Log(level, message);
        }

        public void Log(LogLevel level, string message, string client, string facility)
        {
            _logger.Log(level, message, client, facility);
        }
    }
}