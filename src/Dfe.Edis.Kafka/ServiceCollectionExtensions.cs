using System.Net.Http;
using System.Text.Json;
using Dfe.Edis.Kafka.Producer;
using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Dfe.Edis.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaProducer(this ServiceCollection services, JsonSerializerOptions jsonSerializerOptions = null)
        {
            if (jsonSerializerOptions == null)
            {
                jsonSerializerOptions = new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                };
            }

            services.AddSingleton<KafkaProducerConnection>();
            services.AddScoped<ISchemaRegistryClient>(serviceProvider =>
            {
                var httpClientFactory = serviceProvider.GetService<IHttpClientFactory>();
                var httpClient = httpClientFactory.CreateClient();

                var schemaRegistryConfiguration = serviceProvider.GetService<KafkaSchemaRegistryConfiguration>();

                return new SchemaRegistryClient(httpClient, schemaRegistryConfiguration);
            });
            services.AddScoped<IKafkaSerializerFactory>(serviceProvider =>
            {
                var schemaRegistryClient = serviceProvider.GetService<ISchemaRegistryClient>();
                return new KafkaSerializerFactory(schemaRegistryClient, jsonSerializerOptions);
            });
            services.AddScoped(typeof(IKafkaProducer<,>), typeof(KafkaProducer<,>));
        }
    }
}