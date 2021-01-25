using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Producer
{
    public interface IProducerLogger
    {
        void LogMessage(IProducer<byte[], byte[]> producer, LogMessage logMessage);
    }
}