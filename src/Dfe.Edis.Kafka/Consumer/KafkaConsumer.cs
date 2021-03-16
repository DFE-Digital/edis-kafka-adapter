using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Logging;
using Dfe.Edis.Kafka.Serialization;

namespace Dfe.Edis.Kafka.Consumer
{
    public interface IKafkaConsumer<TKey, TValue>
    {
        void SetMessageHandler(Func<ConsumedMessage<TKey, TValue>, CancellationToken, Task> messageHandler);
        void SetEndOfPartitionHandler(Func<CancellationToken, Task> endOfPartitionHandler);
        Task RunAsync(string topic, CancellationToken cancellationToken);
    }

    public class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        private readonly KafkaConsumerConfiguration _configuration;
        private readonly IKafkaLogger<KafkaConsumer<TKey, TValue>> _logger;
        private readonly IConsumer<TKey, TValue> _consumer;
        private Func<ConsumedMessage<TKey, TValue>, CancellationToken, Task> _messageHandler;
        private Func<CancellationToken, Task> _endOfPartitionHandler;

        internal KafkaConsumer(
            IConsumerBuilderWrapper<TKey, TValue> consumerBuilder,
            KafkaConsumerConfiguration configuration,
            IKafkaDeserializerFactory deserializerFactory,
            IKafkaLogger<KafkaConsumer<TKey, TValue>> logger)
        {
            _configuration = configuration;
            _logger = logger;

            consumerBuilder.SetValueDeserializer(deserializerFactory.GetValueDeserializer<TValue>());
            _consumer = consumerBuilder.Build();
        }

        public KafkaConsumer(
            KafkaConsumerConfiguration configuration,
            IKafkaDeserializerFactory deserializerFactory,
            IKafkaLogger<KafkaConsumer<TKey, TValue>> logger)
            : this(new ConsumerBuilderWrapper<TKey, TValue>(configuration, logger), configuration, deserializerFactory, logger)
        {
        }

        public void SetMessageHandler(Func<ConsumedMessage<TKey, TValue>, CancellationToken, Task> messageHandler)
        {
            _messageHandler = messageHandler;
        }

        public void SetEndOfPartitionHandler(Func<CancellationToken, Task> endOfPartitionHandler)
        {
            _endOfPartitionHandler = endOfPartitionHandler;
        }

        public Task RunAsync(string topic, CancellationToken cancellationToken)
        {
            if (_messageHandler == null)
            {
                throw new NullReferenceException("Must set message handler before running");
            }

            return Task.Factory.StartNew(() =>
            {
                _consumer.Subscribe(topic);

                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        ConsumeNextAsync(cancellationToken).Wait(cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Do nothing. We shutting down
                }
                finally
                {
                    _consumer.Unsubscribe();
                }
            }, cancellationToken);
        }

        private async Task ConsumeNextAsync(CancellationToken cancellationToken)
        {
            var result = _consumer.Consume(cancellationToken);

            if (result.IsPartitionEOF)
            {
                if (_endOfPartitionHandler != null)
                {
                    _logger.Log(LogLevel.Info, $"Reached the end of partition {result.Partition} for topic {result.Topic}. Calling handler");
                    await _endOfPartitionHandler.Invoke(cancellationToken);
                }

                _logger.Log(LogLevel.Info,
                    $"Reached the end of partition {result.Partition} for topic {result.Topic}. Waiting {_configuration.WaitInMsOnPartitionEnd}");
                Task.Delay(_configuration.WaitInMsOnPartitionEnd, cancellationToken).Wait(cancellationToken);
                return;
            }

            var message = new ConsumedMessage<TKey, TValue>
            {
                Topic = result.Topic,
                Partition = result.Partition,
                Offset = result.Offset,
                Key = result.Message.Key,
                Value = result.Message.Value,
            };

            await _messageHandler.Invoke(message, cancellationToken);
        }
    }
}