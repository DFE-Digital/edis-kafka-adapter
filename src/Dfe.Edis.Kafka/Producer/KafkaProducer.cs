using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
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

        internal KafkaProducer(IProducerBuilderWrapper<TKey, TValue> producerBuilder)
        {
            producerBuilder.SetValueSerializer(new KafkaJsonSerializer<TValue>());
            _producer = producerBuilder.Build();
        }
        public KafkaProducer(KafkaProducerConnection connection)
            : this(new ProducerBuilderWrapper<TKey, TValue>(connection))
        {
        }

        public async Task<ProduceResult> ProduceAsync(string topic, TKey key, TValue value, CancellationToken cancellationToken)
        {
            var message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
            };

            var result = await _producer.ProduceAsync(topic, message, cancellationToken);
            return new ProduceResult
            {
                Topic = result.Topic,
                Partition = result.Partition,
                Offset = result.Offset,
            };
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