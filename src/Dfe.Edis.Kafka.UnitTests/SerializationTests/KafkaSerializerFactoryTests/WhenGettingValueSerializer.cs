using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;
using Moq;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.SerializationTests.KafkaSerializerFactoryTests
{
    public class WhenGettingValueSerializer
    {
        [Test]
        public void ThenItShouldReturnInstanceOfJsonSerializer()
        {
            var schemaRegistryClient = new Mock<ISchemaRegistryClient>();
            var factory = new KafkaSerializerFactory(schemaRegistryClient.Object);

            var actual = factory.GetValueSerializer<BasicObject>();
            
            Assert.IsNotNull(actual);
            Assert.IsInstanceOf<KafkaJsonSerializer<BasicObject>>(actual);
        }
    }
}