using System;
using Dfe.Edis.Kafka.Producer;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.ServiceCollectionExtensionsTests
{
    public class WhenAddingKafkaProducer
    {
        [Test]
        public void ThenShouldBeAbleToResolveAProducerOfDifferentTypes()
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddHttpClient();
            serviceCollection.AddSingleton<KafkaBrokerConfiguration>();
            serviceCollection.AddSingleton(new KafkaSchemaRegistryConfiguration
            {
                BaseUrl = "https://localhost:12345/schema-reg/"
            });

            serviceCollection.AddKafkaProducer();
            var serviceProvider = serviceCollection.BuildServiceProvider();

            Assert.IsNotNull(serviceProvider.GetService<IKafkaProducer<string, string>>());
            Assert.IsNotNull(serviceProvider.GetService<IKafkaProducer<long, DateTime>>());
            Assert.IsNotNull(serviceProvider.GetService<IKafkaProducer<int, BasicObject>>());
        }
    }
}