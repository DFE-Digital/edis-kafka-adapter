using System;
using System.Threading;
using AutoFixture.NUnit3;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Producer;
using Moq;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.ProducerTests.KafkaProducerTests
{
    public class WhenFlushing
    {
        [Test, AutoData]
        public void ThenItShouldFlushUnderlyingProducer(TimeSpan timeout)
        {
            // Arrange
            var producerMock = new Mock<IProducer<string, BasicObject>>();
            producerMock.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, BasicObject>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryReport<string, BasicObject>
                {
                    Topic = "some-topic",
                    Partition = 1,
                    Offset = 2,
                });

            var producerBuilderMock = new Mock<IProducerBuilderWrapper<string, BasicObject>>();
            producerBuilderMock.Setup(b => b.SetValueSerializer(It.IsAny<IAsyncSerializer<BasicObject>>()))
                .Returns(producerBuilderMock.Object);
            producerBuilderMock.Setup(b => b.Build())
                .Returns(producerMock.Object);

            var kafkaProducer = new KafkaProducer<string, BasicObject>(producerBuilderMock.Object);

            // Act
            kafkaProducer.Flush(timeout);

            // Assert
            producerMock.Verify(p => p.Flush(timeout), Times.Once);
        }
    }
}