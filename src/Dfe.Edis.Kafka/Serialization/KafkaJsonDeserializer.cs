using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Serialization
{
    internal class KafkaJsonDeserializer<T> : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _serializerOptions;

        public KafkaJsonDeserializer(JsonSerializerOptions serializerOptions)
        {
            _serializerOptions = serializerOptions;
        }

        public async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
            {
                return default;
            }

            using (var stream = new MemoryStream(data.ToArray()))
            {
                return await JsonSerializer.DeserializeAsync<T>(stream, _serializerOptions);
            }
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
            {
                return default;
            }

            using (var stream = new MemoryStream(data.ToArray()))
            {
                return JsonSerializer.DeserializeAsync<T>(stream, _serializerOptions).Result;
            }
        }
    }
}