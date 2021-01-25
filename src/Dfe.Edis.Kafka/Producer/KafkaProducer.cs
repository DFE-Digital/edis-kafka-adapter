using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;

namespace Dfe.Edis.Kafka.Producer
{
    public interface IKafkaProducer<TKey, TValue> : IDisposable
    {
        Task<ProduceResult> ProduceAsync(string topic, TKey key, TValue value, CancellationToken cancellationToken);
        void Flush(TimeSpan timeout);
    }

    public class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
    {
        private readonly IProducer<TKey, TValue> _producer;

        internal KafkaProducer(
            IProducerBuilderWrapper<TKey, TValue> producerBuilder,
            IKafkaSerializerFactory serializerFactory)
        {
            producerBuilder.SetValueSerializer(serializerFactory.GetValueSerializer<TValue>());
            _producer = producerBuilder.Build();
        }

        public KafkaProducer(KafkaProducerConnection connection, IKafkaSerializerFactory serializerFactory)
            : this(new ProducerBuilderWrapper<TKey, TValue>(connection), serializerFactory)
        {
        }

        public async Task<ProduceResult> ProduceAsync(string topic, TKey key, TValue value, CancellationToken cancellationToken)
        {
            var message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
            };

            try
            {
                var result = await _producer.ProduceAsync(topic, message, cancellationToken);
                return new ProduceResult
                {
                    Topic = result.Topic,
                    Partition = result.Partition,
                    Offset = result.Offset,
                };
            }
            catch (ProduceException<TKey, TValue> ex)
            {
                if (ex.InnerException is KafkaSerializationException serializationException)
                {
                    throw serializationException;
                }

                throw;
            }
        }

        public void Flush(TimeSpan timeout)
        {
            _producer.Flush(timeout);
        }

        public void Dispose()
        {
            _producer?.Dispose();
        }
    }
}