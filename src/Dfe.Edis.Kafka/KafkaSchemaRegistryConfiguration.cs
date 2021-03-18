using System;

namespace Dfe.Edis.Kafka
{
    public class KafkaSchemaRegistryConfiguration
    {
        public string BaseUrl { get; set; }
        public TimeSpan CacheTimeout { get; set; } = new TimeSpan(0, 0, 30, 0);
        
        public string Username { get; set; }
        public string Password { get; set; }
    }
}