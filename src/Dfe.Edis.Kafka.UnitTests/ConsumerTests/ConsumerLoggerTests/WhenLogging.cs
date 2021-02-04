using Confluent.Kafka;
using Dfe.Edis.Kafka.Consumer;
using Dfe.Edis.Kafka.Logging;
using Moq;
using NUnit.Framework;

namespace Dfe.Edis.Kafka.UnitTests.ConsumerTests.ConsumerLoggerTests
{
    public class WhenLogging
    {
        [Test]
        public void ThenItShouldLogMessageDetailsToLogger()
        {
            var loggerMock = new Mock<IKafkaLogger<KafkaConsumer<string, string>>>();
            var consumerLogger = new ConsumerLogger<string, string>(loggerMock.Object);

            consumerLogger.Log(null, new LogMessage("name1", SyslogLevel.Debug, "facility2", "some message"));

            loggerMock.Verify(l => l.Log(LogLevel.Debug, "some message", "name1", "facility2"),
                Times.Once);
        }
    }
}