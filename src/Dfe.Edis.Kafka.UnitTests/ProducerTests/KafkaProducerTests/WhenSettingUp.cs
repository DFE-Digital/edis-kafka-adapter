using Confluent.Kafka;
using Dfe.Edis.Kafka.Producer;
using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;
using Moq;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.ProducerTests.KafkaProducerTests
{
    public class WhenSettingUp
    {
        private Mock<IProducerBuilderWrapper<string, BasicObject>> _producerBuilderMock;
        private Mock<IKafkaSerializerFactory> _serializerFactoryMock;

        [SetUp]
        public void Arrange()
        {
            _producerBuilderMock = new Mock<IProducerBuilderWrapper<string, BasicObject>>();
            _producerBuilderMock.Setup(b => b.SetValueSerializer(It.IsAny<IAsyncSerializer<BasicObject>>()))
                .Returns(_producerBuilderMock.Object);

            _serializerFactoryMock = new Mock<IKafkaSerializerFactory>();
            _serializerFactoryMock.Setup(f => f.GetValueSerializer<BasicObject>())
                .Returns(new Mock<IAsyncSerializer<BasicObject>>().Object);
        }

        [Test]
        public void ThenItShouldSetValueSerializerToInstanceOfValueSerializerFromFactory()
        {
            var serializer = new Mock<IAsyncSerializer<BasicObject>>().Object;
            _serializerFactoryMock.Setup(f => f.GetValueSerializer<BasicObject>())
                .Returns(serializer);
            
            using (new KafkaProducer<string, BasicObject>(_producerBuilderMock.Object, _serializerFactoryMock.Object))
            {
                _producerBuilderMock.Verify(b => b.SetValueSerializer(serializer), Times.Once);
            }
            _serializerFactoryMock.Verify(f=>f.GetValueSerializer<BasicObject>(), Times.Once);
        }

        [Test]
        public void ThenItShouldBuildAProducer()
        {
            using (new KafkaProducer<string, BasicObject>(_producerBuilderMock.Object, _serializerFactoryMock.Object))
            {
                _producerBuilderMock.Verify(b => b.Build(),
                    Times.Once);
            }
        }
    }
}