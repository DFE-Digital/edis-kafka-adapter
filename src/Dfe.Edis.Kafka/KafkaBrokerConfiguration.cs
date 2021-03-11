namespace Dfe.Edis.Kafka
{
    public class KafkaBrokerConfiguration
    {
        public string BootstrapServers { get; set; }

        public string SaslUsername { get; set; }
        public string SaslPassword { get; set; }
    }
}