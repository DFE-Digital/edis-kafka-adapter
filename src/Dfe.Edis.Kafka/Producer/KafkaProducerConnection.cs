using System;
using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Producer
{
    public class KafkaProducerConnection : IDisposable
    {
        private readonly IProducer<byte[], byte[]> _producer;

        public KafkaProducerConnection(KafkaBrokerConfiguration configuration)
        {
            _producer = new ProducerBuilder<byte[], byte[]>(
                new ProducerConfig
                {
                    BootstrapServers = configuration.BootstrapServers,
                }).Build();
        }
        
        internal Handle Handle => _producer.Handle;

        public void Dispose()
        {
            _producer?.Flush();
            _producer?.Dispose();
        }
    }
}