using Confluent.Kafka;
using Dfe.Edis.Kafka.Producer;
using Dfe.Edis.Kafka.Serialization;
using Moq;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.ProducerTests.KafkaProducerTests
{
    public class WhenSettingUp
    {
        private Mock<IProducerBuilderWrapper<string, BasicObject>> _producerBuilderMock;

        [SetUp]
        public void Arrange()
        {
            _producerBuilderMock = new Mock<IProducerBuilderWrapper<string, BasicObject>>();
            _producerBuilderMock.Setup(b => b.SetValueSerializer(It.IsAny<IAsyncSerializer<BasicObject>>()))
                .Returns(_producerBuilderMock.Object);
        }

        [Test]
        public void ThenItShouldSetValueSerializerToJsonSerializer()
        {
            using (new KafkaProducer<string, BasicObject>(_producerBuilderMock.Object))
            {
                _producerBuilderMock.Verify(b => b.SetValueSerializer(It.IsAny<KafkaJsonSerializer<BasicObject>>()),
                    Times.Once);
            }
        }

        [Test]
        public void ThenItShouldBuildAProducer()
        {
            using (new KafkaProducer<string, BasicObject>(_producerBuilderMock.Object))
            {
                _producerBuilderMock.Verify(b => b.Build(),
                    Times.Once);
            }
        }
    }
}