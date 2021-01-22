using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using AutoFixture.NUnit3;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Serialization;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.SerializationTests.KafkaJsonSerializerTests
{
    public class WhenSerializingAsync
    {
        [Test, AutoData]
        public async Task ThenItShouldSerializeAStringCorrectly(string data)
        {
            var serializer = new KafkaJsonSerializer<string>();

            var actual = await serializer.SerializeAsync(data, new SerializationContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }
        
        [Test, AutoData]
        public async Task ThenItShouldSerializeAnIntegerCorrectly(int data)
        {
            var serializer = new KafkaJsonSerializer<int>();

            var actual = await serializer.SerializeAsync(data, new SerializationContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }
        
        [Test, AutoData]
        public async Task ThenItShouldSerializeALongCorrectly(long data)
        {
            var serializer = new KafkaJsonSerializer<long>();

            var actual = await serializer.SerializeAsync(data, new SerializationContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }
        
        [Test, AutoData]
        public async Task ThenItShouldSerializeADateTimeCorrectly(DateTime data)
        {
            var serializer = new KafkaJsonSerializer<DateTime>();

            var actual = await serializer.SerializeAsync(data, new SerializationContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }
        
        [Test, AutoData]
        public async Task ThenItShouldSerializeAnObjectCorrectly(BasicObject data)
        {
            var serializer = new KafkaJsonSerializer<BasicObject>();

            var actual = await serializer.SerializeAsync(data, new SerializationContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }
    }
}