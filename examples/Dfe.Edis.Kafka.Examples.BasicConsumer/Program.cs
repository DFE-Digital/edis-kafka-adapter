using System;
using System.Threading;
using System.Threading.Tasks;
using Dfe.Edis.Kafka.Consumer;
using Dfe.Edis.Kafka.Producer;
using Microsoft.Extensions.DependencyInjection;

namespace Dfe.Edis.Kafka.Examples.BasicConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var cancellationTokenSource = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                Console.WriteLine("Stopping...");
                eventArgs.Cancel = true;
                cancellationTokenSource.Cancel();
            };
            
            
            Console.WriteLine("Setting up; please wait...");

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton(new KafkaConsumerConfiguration
            {
                BootstrapServers = "localhost:9092",
                GroupId = "group-one",
            });
            serviceCollection.AddKafkaConsumer();
            var serviceProvider = serviceCollection.BuildServiceProvider();

            Console.WriteLine("Listening...");
            var consumer = serviceProvider.GetService<IKafkaConsumer<string, string>>();
            consumer.SetMessageHandler(MessageReceived);
            consumer.RunAsync("test-messages", cancellationTokenSource.Token).Wait();
            
            Console.WriteLine("Bye!");
        }

        static Task MessageReceived(ConsumedMessage<string, string> message, CancellationToken cancellationToken)
        {
            Console.WriteLine($"[P:{message.Partition}, O:{message.Offset}]: {message.Key} / {message.Value}");
            return Task.CompletedTask;
        }
    }
}