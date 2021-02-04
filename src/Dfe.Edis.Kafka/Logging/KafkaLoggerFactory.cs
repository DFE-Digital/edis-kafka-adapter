using System;
using Microsoft.Extensions.Logging;

namespace Dfe.Edis.Kafka.Logging
{
    internal class KafkaLoggerFactory
    {
        private readonly Func<Type, object> _getService;

        public KafkaLoggerFactory(Func<Type, object> getService)
        {
            _getService = getService;
        }

        public IKafkaLogger<T> GetLogger<T>()
        {
            var microsoftLogger = (ILogger<T>) _getService(typeof(ILogger<T>));
            if (microsoftLogger != null)
            {
                return new MicrosoftLoggingKafkaLogger<T>(microsoftLogger);
            }

            return new NoopLogger<T>();
        }
    }
}