using System;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Logging;
using Dfe.Edis.Kafka.OAuth;

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

            if (configuration.AuthenticationType == KafkaAuthenticationType.SaslSslPlainText)
            {
                producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                producerConfig.SaslMechanism = SaslMechanism.Plain;
                producerConfig.SaslUsername = configuration.Username;
                producerConfig.SaslPassword = configuration.Password;
            }
            else if (configuration.AuthenticationType == KafkaAuthenticationType.SaslPlainTextOAuth)
            {
                producerConfig.SecurityProtocol = SecurityProtocol.SaslPlaintext;
                producerConfig.SaslMechanism = SaslMechanism.OAuthBearer;
            }

            var producerBuilder = new ProducerBuilder<byte[], byte[]>(
                    producerConfig)
                .SetLogHandler(producerLogger.LogMessage);
            if (configuration.AuthenticationType == KafkaAuthenticationType.SaslPlainTextOAuth)
            {
                // TODO: Maybe make this injectable for testing
                var tokenClient = new KafkaOAuthTokenClient<byte[], byte[]>(configuration, logger);
                producerBuilder.SetOAuthBearerTokenRefreshHandler(tokenClient.RefreshProducerToken);
            }
            
            _producer = producerBuilder.Build();
        }

        internal Handle Handle => _producer.Handle;

        public void Dispose()
        {
            _producer?.Flush();
            _producer?.Dispose();
        }
    }
}