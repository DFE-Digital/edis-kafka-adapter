using Dfe.Edis.Kafka.Producer;
using Microsoft.Extensions.DependencyInjection;

namespace Dfe.Edis.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static void AddKafkaProducer(this ServiceCollection services)
        {
            services.AddSingleton<KafkaProducerConnection>();
            services.AddScoped(typeof(IKafkaProducer<,>), typeof(KafkaProducer<,>));
        }
    }
}