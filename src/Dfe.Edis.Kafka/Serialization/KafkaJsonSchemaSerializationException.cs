namespace Dfe.Edis.Kafka.Serialization
{
    public class KafkaJsonSchemaSerializationException : KafkaSerializationException
    {
        public string[] ValidationErrors { get; }

        public KafkaJsonSchemaSerializationException(string[] validationErrors)
            : base($"Value did not conform to schema. {validationErrors.Length} errors found (See ValidationErrors for more details)")
        {
            ValidationErrors = validationErrors;
        }
    }
}