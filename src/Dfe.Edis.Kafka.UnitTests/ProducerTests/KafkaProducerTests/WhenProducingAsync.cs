using System.Threading;
using System.Threading.Tasks;
using AutoFixture.NUnit3;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Producer;
using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;
using Moq;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.ProducerTests.KafkaProducerTests
{
    public class WhenProducingAsync
    {
        private Mock<IProducerBuilderWrapper<string, BasicObject>> _producerBuilderMock;
        private Mock<IProducer<string, BasicObject>> _producerMock;
        private Mock<IKafkaSerializerFactory> _serializerFactoryMock;
        private KafkaProducer<string, BasicObject> _kafkaProducer;

        [SetUp]
        public void Arrange()
        {
            _producerMock = new Mock<IProducer<string, BasicObject>>();
            _producerMock.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, BasicObject>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryReport<string, BasicObject>
                {
                    Topic = "some-topic",
                    Partition = 1,
                    Offset = 2,
                });

            _producerBuilderMock = new Mock<IProducerBuilderWrapper<string, BasicObject>>();
            _producerBuilderMock.Setup(b => b.SetValueSerializer(It.IsAny<IAsyncSerializer<BasicObject>>()))
                .Returns(_producerBuilderMock.Object);
            _producerBuilderMock.Setup(b => b.Build())
                .Returns(_producerMock.Object);

            _serializerFactoryMock = new Mock<IKafkaSerializerFactory>();
            _serializerFactoryMock.Setup(f => f.GetValueSerializer<BasicObject>())
                .Returns(new Mock<IAsyncSerializer<BasicObject>>().Object);

            _kafkaProducer = new KafkaProducer<string, BasicObject>(_producerBuilderMock.Object, _serializerFactoryMock.Object);
        }

        [Test, AutoData]
        public async Task ThenItShouldProduceMessageToUnderlyingProducer(string topic, string key, BasicObject value)
        {
            var cancellationToken = new CancellationToken();

            await _kafkaProducer.ProduceAsync(topic, key, value, cancellationToken);

            _producerMock.Verify(p => p.ProduceAsync(
                    topic,
                    It.Is<Message<string, BasicObject>>(message => message.Key == key && message.Value == value),
                    cancellationToken),
                Times.Once);
        }

        [Test, AutoData]
        public async Task ThenItShouldReturnDeliveryInformationFromUnderlyingProducer(
            string topic, 
            string key, 
            BasicObject value, 
            int expectedPartition, 
            int expectedOffset)
        {
            _producerMock.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, BasicObject>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryReport<string, BasicObject>
                {
                    Topic = topic,
                    Partition = expectedPartition,
                    Offset = expectedOffset,
                });

            var actual = await _kafkaProducer.ProduceAsync(topic, key, value, CancellationToken.None);
            
            Assert.IsNotNull(actual);
            Assert.AreEqual(topic, actual.Topic);
            Assert.AreEqual(expectedPartition, actual.Partition);
            Assert.AreEqual(expectedOffset, actual.Offset);
        }
    }
}