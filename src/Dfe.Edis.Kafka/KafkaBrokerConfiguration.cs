namespace Dfe.Edis.Kafka
{
    public class KafkaBrokerConfiguration
    {
        public string BootstrapServers { get; set; }

        public KafkaAuthenticationType AuthenticationType { get; set; } = KafkaAuthenticationType.None;
        public string Username { get; set; }
        public string Password { get; set; }
        public string MdsUrl { get; set; }
    }
}