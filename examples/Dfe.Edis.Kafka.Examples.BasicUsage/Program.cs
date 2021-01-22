using System;
using System.Threading;
using Dfe.Edis.Kafka.Producer;
using Microsoft.Extensions.DependencyInjection;

namespace Dfe.Edis.Kafka.Examples.BasicUsage
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Setting up; please wait...");

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton(new KafkaBrokerConfiguration
            {
                BootstrapServers = "localhost:9092"
            });
            serviceCollection.AddKafkaProducer();
            var serviceProvider = serviceCollection.BuildServiceProvider();

            var producer = serviceProvider.GetService<IKafkaProducer<string, string>>();
            if (producer == null)
            {
                Console.WriteLine("Failed to get producer!");
                return;
            }
            
            Console.WriteLine("Ready!");

            var iteration = 0;
            var message = GetMessage(iteration);
            while (!message.Equals("exit", StringComparison.InvariantCultureIgnoreCase))
            {
                var result = producer.ProduceAsync("test-messages", DateTime.Now.Ticks.ToString(), message, CancellationToken.None).Result;
                Console.WriteLine($"Messages sent to topic {result.Topic} at offset {result.Offset} in partition {result.Partition}");

                iteration++;
                message = GetMessage(iteration);
            }
        }

        static string GetMessage(int iteration)
        {
            if (iteration == 0)
            {
                Console.Write("Enter a message or exit to end: ");
            }
            else
            {
                Console.Write("Enter another message or exit to end: ");
            }

            return Console.ReadLine();
        }
    }
}