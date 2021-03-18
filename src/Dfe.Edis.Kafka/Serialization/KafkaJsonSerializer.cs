using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dfe.Edis.Kafka.SchemaRegistry;
using NJsonSchema;

namespace Dfe.Edis.Kafka.Serialization
{
    internal class KafkaJsonSerializer<T> : IAsyncSerializer<T>
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly JsonSerializerOptions _serializerOptions;

        public KafkaJsonSerializer(ISchemaRegistryClient schemaRegistryClient, JsonSerializerOptions serializerOptions)
        {
            _schemaRegistryClient = schemaRegistryClient;
            _serializerOptions = serializerOptions;
        }
        
        public async Task<byte[]> SerializeAsync(T data, SerializationContext context)
        {
            var json = JsonSerializer.Serialize(data, new JsonSerializerOptions // TODO: Make this configurable
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                IgnoreNullValues = true,
            });

            var subjectName = $"{context.Topic}-{context.Component.ToString().ToLower()}";
            var subjectVersions = await _schemaRegistryClient.ListSchemaVersionsAsync(subjectName, CancellationToken.None);
            if (subjectVersions != null && subjectVersions.Any())
            {
                var version = subjectVersions.Last();
                var schemaDetails = await _schemaRegistryClient.GetSchemaAsync(subjectName, version, CancellationToken.None);
                if (!schemaDetails.SchemaType.Equals("JSON", StringComparison.InvariantCultureIgnoreCase))
                {
                    throw new KafkaSerializationException($"Unable to verify schema for subject {subjectName}, version {version}, " +
                                                          $"as the schema is {schemaDetails.SchemaType} but expected JSON");
                }

                var schema = await JsonSchema.FromJsonAsync(schemaDetails.Schema);
                var validationErrors = schema.Validate(json);
                if (validationErrors.Any())
                {
                    var validationErrorStrings = validationErrors.Select(err =>
                        err.ToString()).ToArray();
                    throw new KafkaJsonSchemaSerializationException(validationErrorStrings);
                }
            }

            return Encoding.UTF8.GetBytes(json);
        }
    }
}