using System.Text.Json;
using Dfe.Edis.Kafka.Serialization;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.SerializationTests.KafkaDeserializerFactoryTests
{
    public class WhenGettingValueDeserializer
    {
        [Test]
        public void ThenItShouldReturnInstanceOfJsonSerializer()
        {
            var factory = new KafkaDeserializerFactory(new JsonSerializerOptions());

            var actual = factory.GetValueDeserializer<BasicObject>();
            
            Assert.IsNotNull(actual);
            Assert.IsInstanceOf<KafkaJsonDeserializer<BasicObject>>(actual);
        }
    }
}