using System.Text.Json;
using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Serialization
{
    public interface IKafkaDeserializerFactory
    {
        IAsyncDeserializer<T> GetValueDeserializer<T>();
    }
    
    public class KafkaDeserializerFactory
    {
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public KafkaDeserializerFactory(JsonSerializerOptions jsonSerializerOptions)
        {
            _jsonSerializerOptions = jsonSerializerOptions;
        }
        
        public IAsyncDeserializer<T> GetValueDeserializer<T>()
        {
            return new KafkaJsonDeserializer<T>(_jsonSerializerOptions);
        }
    }
}