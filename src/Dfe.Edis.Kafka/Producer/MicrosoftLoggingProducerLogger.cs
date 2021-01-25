using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Dfe.Edis.Kafka.Producer
{
    public class MicrosoftLoggingProducerLogger: IProducerLogger
    {
        private readonly ILogger<KafkaProducerConnection> _logger;

        public MicrosoftLoggingProducerLogger(ILogger<KafkaProducerConnection> logger)
        {
            _logger = logger;
        }
        
        public void LogMessage(IProducer<byte[], byte[]> producer, LogMessage logMessage)
        {
            switch (logMessage.Level)
            {
                case SyslogLevel.Emergency:
                case SyslogLevel.Alert:
                case SyslogLevel.Critical:
                    _logger.LogCritical(logMessage.Message);
                    break;
                case SyslogLevel.Error:
                    _logger.LogError(logMessage.Message);
                    break;
                case SyslogLevel.Warning:
                    _logger.LogWarning(logMessage.Message);
                    break;
                case SyslogLevel.Notice:
                case SyslogLevel.Info:
                    _logger.LogInformation(logMessage.Message);
                    break;
                default:
                    _logger.LogDebug(logMessage.Message);
                    break;
            }
        }
    }
}