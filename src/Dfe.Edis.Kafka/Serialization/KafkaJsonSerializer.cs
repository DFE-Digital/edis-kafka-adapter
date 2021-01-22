using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Dfe.Edis.Kafka.Serialization
{
    internal class KafkaJsonSerializer<T> : IAsyncSerializer<T>
    {
        public async Task<byte[]> SerializeAsync(T data, SerializationContext context)
        {
            var json = JsonSerializer.Serialize(data);
            
            // TODO: Check against schema registry

            return Encoding.UTF8.GetBytes(json);
        }
    }
}