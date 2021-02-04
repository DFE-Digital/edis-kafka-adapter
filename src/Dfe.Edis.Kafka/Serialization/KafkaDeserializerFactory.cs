using System.Text.Json;
using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Serialization
{
    public interface IKafkaDeserializerFactory
    {
        IDeserializer<T> GetValueDeserializer<T>();
    }
    
    public class KafkaDeserializerFactory : IKafkaDeserializerFactory
    {
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public KafkaDeserializerFactory(JsonSerializerOptions jsonSerializerOptions)
        {
            _jsonSerializerOptions = jsonSerializerOptions;
        }
        
        public IDeserializer<T> GetValueDeserializer<T>()
        {
            return new KafkaJsonDeserializer<T>(_jsonSerializerOptions);
        }
    }
}