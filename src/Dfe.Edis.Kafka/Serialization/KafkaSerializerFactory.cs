using System.Text.Json;
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
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public KafkaSerializerFactory(ISchemaRegistryClient schemaRegistryClient, JsonSerializerOptions jsonSerializerOptions)
        {
            _schemaRegistryClient = schemaRegistryClient;
            _jsonSerializerOptions = jsonSerializerOptions;
        }
        
        public IAsyncSerializer<T> GetValueSerializer<T>()
        {
            return new KafkaJsonSerializer<T>(_schemaRegistryClient, _jsonSerializerOptions);
        }
    }
}