namespace Dfe.Edis.Kafka.Producer
{
    public class ProduceResult
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
    }
}