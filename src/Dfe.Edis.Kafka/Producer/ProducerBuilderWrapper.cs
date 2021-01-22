using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Producer
{
    internal interface IProducerBuilderWrapper<TKey, TValue>
    {
        IProducerBuilderWrapper<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer);
        IProducer<TKey, TValue> Build();
    }

    internal class ProducerBuilderWrapper<TKey, TValue> : IProducerBuilderWrapper<TKey, TValue>
    {
        private DependentProducerBuilder<TKey, TValue> _dependentProducerBuilder;

        public ProducerBuilderWrapper(KafkaProducerConnection connection)
        {
            _dependentProducerBuilder = new DependentProducerBuilder<TKey, TValue>(connection.Handle);
        }
        
        public IProducerBuilderWrapper<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer)
        {
            _dependentProducerBuilder.SetValueSerializer(serializer);
            return this;
        }

        public IProducer<TKey, TValue> Build()
        {
            return _dependentProducerBuilder.Build();
        }
    }
}