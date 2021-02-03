using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using AutoFixture.NUnit3;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Serialization;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.SerializationTests.KafkaJsonDeserializerTests
{
    public class WhenDeserializingAsync
    {
        [Test]
        public async Task AndIsNullIsTrueThenItShouldReturnNull()
        {
            var deserializer = new KafkaJsonDeserializer<BasicObject>(new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            });

            var actual = await deserializer.DeserializeAsync(null, true, new SerializationContext());
            
            Assert.IsNull(actual);
        }

        [Test, AutoData]
        public async Task ThenItShouldReturnDeserializedResult(BasicObject expected)
        {
            var serializerOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            };
            var deserializer = new KafkaJsonDeserializer<BasicObject>(serializerOptions);

            var buffer = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(expected, serializerOptions));
            var actual = await deserializer.DeserializeAsync(new ReadOnlyMemory<byte>(buffer), false, new SerializationContext());
            
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.Id, actual.Id);
            Assert.AreEqual(expected.Name, actual.Name);
            Assert.AreEqual(expected.TimeStamp, actual.TimeStamp);
        }
    }
}