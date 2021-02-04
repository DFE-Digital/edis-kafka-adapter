namespace Dfe.Edis.Kafka.Consumer
{
    public class ConsumedMessage<TKey, TValue>
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
        public TKey Key { get; set; }
        public TValue Value { get; set; }
    }
}