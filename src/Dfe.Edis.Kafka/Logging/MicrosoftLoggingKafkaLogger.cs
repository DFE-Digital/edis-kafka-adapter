using Microsoft.Extensions.Logging;

namespace Dfe.Edis.Kafka.Logging
{
    public class MicrosoftLoggingKafkaLogger<T> : IKafkaLogger<T>
    {
        private readonly ILogger<T> _logger;

        public MicrosoftLoggingKafkaLogger(ILogger<T> logger)
        {
            _logger = logger;
        }

        public void Log(LogLevel level, string message)
        {
            Log(level, message, null, null);
        }

        public void Log(LogLevel level, string message, string client, string facility)
        {
            var messageFormat = "{Message} (client: {Client}, facility: {Facility})";
            switch (level)
            {
                case LogLevel.Emergency:
                case LogLevel.Alert:
                case LogLevel.Critical:
                    _logger.LogCritical(messageFormat, message, client, facility);
                    break;
                case LogLevel.Error:
                    _logger.LogError(messageFormat, message, client, facility);
                    break;
                case LogLevel.Warning:
                    _logger.LogWarning(messageFormat, message, client, facility);
                    break;
                case LogLevel.Notice:
                case LogLevel.Info:
                    _logger.LogInformation(messageFormat, message, client, facility);
                    break;
                default:
                    _logger.LogDebug(messageFormat, message, client, facility);
                    break;
            }
        }
    }
}