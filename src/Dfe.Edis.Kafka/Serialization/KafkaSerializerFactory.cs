using Confluent.Kafka;
using Dfe.Edis.Kafka.SchemaRegistry;

namespace Dfe.Edis.Kafka.Serialization
{
    public interface IKafkaSerializerFactory
    {
        IAsyncSerializer<T> GetValueSerializer<T>();
    }

    public class KafkaSerializerFactory : IKafkaSerializerFactory
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;

        public KafkaSerializerFactory(ISchemaRegistryClient schemaRegistryClient)
        {
            _schemaRegistryClient = schemaRegistryClient;
        }
        
        public IAsyncSerializer<T> GetValueSerializer<T>()
        {
            return new KafkaJsonSerializer<T>(_schemaRegistryClient);
        }
    }
}