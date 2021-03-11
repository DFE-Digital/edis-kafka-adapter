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
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration.BootstrapServers,
            };
            if (!string.IsNullOrEmpty(configuration.SaslUsername))
            {
                producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                producerConfig.SaslMechanism = SaslMechanism.Plain;
                producerConfig.SaslUsername = configuration.SaslUsername;
                producerConfig.SaslPassword = configuration.SaslPassword;
            }
            _producer = new ProducerBuilder<byte[], byte[]>(
                    producerConfig)
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