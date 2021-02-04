using Confluent.Kafka;
using Dfe.Edis.Kafka.Logging;

namespace Dfe.Edis.Kafka.Consumer
{
    internal interface IConsumerBuilderWrapper<TKey, TValue>
    {
        void SetValueDeserializer(IDeserializer<TValue> deserializer);
        IConsumer<TKey, TValue> Build();
    }

    internal class ConsumerBuilderWrapper<TKey, TValue> : IConsumerBuilderWrapper<TKey, TValue>
    {
        private ConsumerBuilder<TKey, TValue> _builder;

        public ConsumerBuilderWrapper(
            KafkaConsumerConfiguration configuration,
            IKafkaLogger<KafkaConsumer<TKey, TValue>> logger)
        {
            var consumerLogger = new ConsumerLogger<TKey, TValue>(logger);
            _builder = new ConsumerBuilder<TKey, TValue>(new ConsumerConfig
            {
                BootstrapServers = configuration.BootstrapServers,
                GroupId = configuration.GroupId,
                EnableAutoCommit = true,
                EnablePartitionEof = true,
            }).SetLogHandler(consumerLogger.Log);
        }
        
        public void SetValueDeserializer(IDeserializer<TValue> deserializer)
        {
            _builder.SetValueDeserializer(deserializer);
        }

        public IConsumer<TKey, TValue> Build()
        {
            return _builder.Build();
        }
    }
}