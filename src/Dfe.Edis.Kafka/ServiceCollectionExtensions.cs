using System.Net.Http;
using System.Text.Json;
using Dfe.Edis.Kafka.Producer;
using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Dfe.Edis.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaProducer(this IServiceCollection services, JsonSerializerOptions jsonSerializerOptions = null)
        {
            if (jsonSerializerOptions == null)
            {
                jsonSerializerOptions = new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                };
            }

            services.AddSingleton<IProducerLogger>(serviceProvider =>
            {
                var microsoftLogger = serviceProvider.GetService<ILogger<KafkaProducerConnection>>();
                if (microsoftLogger != null)
                {
                    return new MicrosoftLoggingProducerLogger(microsoftLogger);
                }

                return new NoopProducerLogger();
            });
            services.AddSingleton<KafkaProducerConnection>();
            services.AddSingleton<ISchemaRegistryClient>(serviceProvider =>
            {
                var httpClientFactory = serviceProvider.GetService<IHttpClientFactory>();
                var httpClient = httpClientFactory.CreateClient();

                var schemaRegistryConfiguration = serviceProvider.GetService<KafkaSchemaRegistryConfiguration>();

                var httpSchemaClient = new SchemaRegistryClient(httpClient, schemaRegistryConfiguration);

                return schemaRegistryConfiguration.CacheTimeout.TotalSeconds > 0
                    ? (ISchemaRegistryClient)new CachedSchemaRegistryClient(httpSchemaClient, schemaRegistryConfiguration)
                    : httpSchemaClient;
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