namespace Dfe.Edis.Kafka
{
    public class KafkaConsumerConfiguration : KafkaBrokerConfiguration
    {
        public string GroupId { get; set; }
        public int WaitInMsOnPartitionEnd { get; set; } = 1000;
        public bool StartAtEarliestOffset { get; set; }
    }
}