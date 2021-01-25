using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Producer
{
    public class NoopProducerLogger : IProducerLogger
    {
        public void LogMessage(IProducer<byte[], byte[]> producer, LogMessage logMessage)
        {
            throw new System.NotImplementedException();
        }
    }
}