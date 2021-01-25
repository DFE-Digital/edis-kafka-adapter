using System.Net.Http;
using Dfe.Edis.Kafka.Producer;
using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Dfe.Edis.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaProducer(this ServiceCollection services)
        {
            services.AddSingleton<KafkaProducerConnection>();
            services.AddScoped<ISchemaRegistryClient>(serviceProvider =>
            {
                var httpClientFactory = serviceProvider.GetService<IHttpClientFactory>();
                var httpClient = httpClientFactory.CreateClient();

                var schemaRegistryConfiguration = serviceProvider.GetService<KafkaSchemaRegistryConfiguration>();

                return new SchemaRegistryClient(httpClient, schemaRegistryConfiguration);
            });
            services.AddScoped<IKafkaSerializerFactory, KafkaSerializerFactory>();
            services.AddScoped(typeof(IKafkaProducer<,>), typeof(KafkaProducer<,>));
        }
    }
}