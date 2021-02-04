using System;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Logging;

namespace Dfe.Edis.Kafka.Producer
{
    public class KafkaProducerConnection : IDisposable
    {
        private readonly IProducer<byte[], byte[]> _producer;

        public KafkaProducerConnection(KafkaBrokerConfiguration configuration, IKafkaLogger<KafkaProducerConnection> logger)
        {
            var producerLogger = new ProducerLogger(logger);
            _producer = new ProducerBuilder<byte[], byte[]>(
                    new ProducerConfig
                    {
                        BootstrapServers = configuration.BootstrapServers,
                    })
                .SetLogHandler(producerLogger.LogMessage)
                .Build();
        }

        internal Handle Handle => _producer.Handle;

        public void Dispose()
        {
            _producer?.Flush();
            _producer?.Dispose();
        }
    }
}