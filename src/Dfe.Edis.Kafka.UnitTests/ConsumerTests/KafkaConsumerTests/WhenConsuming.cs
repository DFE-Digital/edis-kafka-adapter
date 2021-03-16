using System;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture.NUnit3;
using Castle.DynamicProxy;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Consumer;
using Dfe.Edis.Kafka.Logging;
using Dfe.Edis.Kafka.Serialization;
using Moq;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.ConsumerTests.KafkaConsumerTests
{
    public class WhenConsuming
    {
        private Mock<IConsumer<string, string>> _innerConsumerMock;
        private Mock<IConsumerBuilderWrapper<string, string>> _consumerBuilderMock;
        private KafkaConsumerConfiguration _configuration;
        private Mock<IDeserializer<string>> _deserializerMock;
        private Mock<IKafkaDeserializerFactory> _deserializerFactoryMock;
        private Mock<IKafkaLogger<KafkaConsumer<string, string>>> _loggerMock;
        private KafkaConsumer<string, string> _consumer;
        private Mock<Func<ConsumedMessage<string, string>, CancellationToken, Task>> _messageHandlerMock;
        private Mock<Func<CancellationToken, Task>> _endOfPartitionHandlerMock;

        [SetUp]
        public void Arrange()
        {
            _innerConsumerMock = new Mock<IConsumer<string, string>>();
            _innerConsumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Returns(new ConsumeResult<string, string> {IsPartitionEOF = true});

            _consumerBuilderMock = new Mock<IConsumerBuilderWrapper<string, string>>();
            _consumerBuilderMock.Setup(b => b.Build())
                .Returns(_innerConsumerMock.Object);

            _configuration = new KafkaConsumerConfiguration();

            _deserializerMock = new Mock<IDeserializer<string>>();

            _deserializerFactoryMock = new Mock<IKafkaDeserializerFactory>();
            _deserializerFactoryMock.Setup(f => f.GetValueDeserializer<string>())
                .Returns(_deserializerMock.Object);

            _loggerMock = new Mock<IKafkaLogger<KafkaConsumer<string, string>>>();

            _consumer = new KafkaConsumer<string, string>(
                _consumerBuilderMock.Object,
                _configuration,
                _deserializerFactoryMock.Object,
                _loggerMock.Object);

            _messageHandlerMock = new Mock<Func<ConsumedMessage<string, string>, CancellationToken, Task>>();
            _messageHandlerMock.Setup(h => h.Invoke(It.IsAny<ConsumedMessage<string, string>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _endOfPartitionHandlerMock = new Mock<Func<CancellationToken, Task>>();
            _endOfPartitionHandlerMock.Setup(h => h.Invoke(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
        }

        [Test]
        public void ThenItShouldBuildConsumerWithValueDeserializer()
        {
            // Act is constructor in main setup

            _deserializerFactoryMock.Verify(f => f.GetValueDeserializer<string>(),
                Times.Once);
            _consumerBuilderMock.Verify(b => b.SetValueDeserializer(_deserializerMock.Object),
                Times.Once);
            _consumerBuilderMock.Verify(b => b.Build(),
                Times.Once);
        }

        [Test]
        public void ThenItShouldThrowExceptionIfNoMessageHandlerSet()
        {
            var actual = Assert.ThrowsAsync<NullReferenceException>(async () =>
                await _consumer.RunAsync("topic", CancellationToken.None));
            Assert.AreEqual("Must set message handler before running", actual.Message);
        }

        [Test, AutoData]
        public async Task ThenItShouldSubscribeToTopic(string topic)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            _consumer.SetMessageHandler(_messageHandlerMock.Object);

            var consumeTask = _consumer.RunAsync(topic, cancellationTokenSource.Token);
            await Task.Delay(10);
            cancellationTokenSource.Cancel();
            await consumeTask;

            _innerConsumerMock.Verify(c => c.Subscribe(topic),
                Times.Once);
        }

        [Test]
        public async Task ThenItShouldConsumeMessagesUntilCancelled()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            _consumer.SetMessageHandler(_messageHandlerMock.Object);

            var consumeTask = _consumer.RunAsync("topic", cancellationTokenSource.Token);
            await Task.Delay(10);
            cancellationTokenSource.Cancel();
            await consumeTask;

            _innerConsumerMock.Verify(c => c.Consume(cancellationTokenSource.Token),
                Times.AtLeast(1));
        }

        [Test]
        public async Task ThenItShouldNotCallMessageHandlerForEndOfPartitionMessages()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            _consumer.SetMessageHandler(_messageHandlerMock.Object);
            _innerConsumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Returns(new ConsumeResult<string, string> {IsPartitionEOF = true});

            var consumeTask = _consumer.RunAsync("topic", cancellationTokenSource.Token);
            await Task.Delay(10);
            cancellationTokenSource.Cancel();
            await consumeTask;

            _messageHandlerMock.Verify(h => h.Invoke(It.IsAny<ConsumedMessage<string, string>>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Test, AutoData]
        public async Task ThenItShouldCallMessageHandlerForMessagesThatAreNotEndOfPartition(string topic, int partition, long offset, string key, string value)
        {
            var consumeCount = 0;

            var cancellationTokenSource = new CancellationTokenSource();
            _consumer.SetMessageHandler(_messageHandlerMock.Object);
            _innerConsumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    consumeCount++;
                    return new ConsumeResult<string, string>
                    {
                        Topic = topic,
                        Partition = partition,
                        Offset = offset,
                        Message = new Message<string, string>
                        {
                            Key = key,
                            Value = value,
                        },
                    };
                });

            var consumeTask = _consumer.RunAsync("topic", cancellationTokenSource.Token);
            await Task.Delay(100);
            cancellationTokenSource.Cancel();
            await consumeTask;

            _messageHandlerMock.Verify(h => h.Invoke(
                    It.Is<ConsumedMessage<string, string>>(message =>
                        message.Topic == topic &&
                        message.Partition == partition &&
                        message.Offset == offset &&
                        message.Key == key &&
                        message.Value == value),
                    cancellationTokenSource.Token),
                Times.Exactly(consumeCount));
        }

        [Test, AutoData]
        public async Task ThenItShouldCallEndOfPartitionHandlerForMessagesThatAreEndOfPartition(string topic, int partition, long offset, string key, string value)
        {
            var consumeCount = 0;
            var eopCount = 0;

            var cancellationTokenSource = new CancellationTokenSource();
            _consumer.SetMessageHandler(_messageHandlerMock.Object);
            _consumer.SetEndOfPartitionHandler(_endOfPartitionHandlerMock.Object);
            _innerConsumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    consumeCount++;
                    if (consumeCount % 3 == 0)
                    {
                        eopCount++;
                        return new ConsumeResult<string, string>
                        {
                            Topic = topic,
                            IsPartitionEOF = true,
                        };
                    }
                    
                    return new ConsumeResult<string, string>
                    {
                        Topic = topic,
                        Partition = partition,
                        Offset = offset,
                        Message = new Message<string, string>
                        {
                            Key = key,
                            Value = value,
                        },
                    };
                });

            var consumeTask = _consumer.RunAsync("topic", cancellationTokenSource.Token);
            await Task.Delay(100);
            cancellationTokenSource.Cancel();
            await consumeTask;

            _endOfPartitionHandlerMock.Verify(h => h.Invoke(cancellationTokenSource.Token),
                Times.Exactly(eopCount));
        }

        [Test]
        public async Task ThenItShouldUnsubscribeWhenDone()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            _consumer.SetMessageHandler(_messageHandlerMock.Object);

            var consumeTask = _consumer.RunAsync("topic", cancellationTokenSource.Token);
            await Task.Delay(10);
            cancellationTokenSource.Cancel();
            await consumeTask;

            _innerConsumerMock.Verify(c => c.Unsubscribe(),
                Times.Once);
        }
    }
}