using Confluent.Kafka;
using Dfe.Edis.Kafka.Logging;

namespace Dfe.Edis.Kafka.Consumer
{
    internal class ConsumerLogger<TKey, TValue>
    {
        private readonly IKafkaLogger<KafkaConsumer<TKey, TValue>> _logger;

        public ConsumerLogger(IKafkaLogger<KafkaConsumer<TKey, TValue>> logger)
        {
            _logger = logger;
        }

        public void Log(IConsumer<TKey, TValue> consumer, LogMessage logMessage)
        {
            _logger.Log(logMessage);
        }
    }
}