using Confluent.Kafka;
using Dfe.Edis.Kafka.Logging;

namespace Dfe.Edis.Kafka.Producer
{
    internal class ProducerLogger
    {
        private readonly IKafkaLogger<KafkaProducerConnection> _logger;

        public ProducerLogger(IKafkaLogger<KafkaProducerConnection> logger)
        {
            _logger = logger;
        }

        public void LogMessage(IProducer<byte[], byte[]> producer, LogMessage logMessage)
        {
            _logger.Log(logMessage);
        }
    }
}