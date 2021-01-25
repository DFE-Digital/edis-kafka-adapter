namespace Dfe.Edis.Kafka.SchemaRegistry
{
    public class SchemaDetails
    {
        public string Subject { get; set; }
        public int Version { get; set; }
        public int Id { get; set; }
        public string SchemaType { get; set; }
        public string Schema { get; set; }
    }
}