using System;

namespace Dfe.Edis.Kafka.SchemaRegistry
{
    public class SchemaRegistryException : Exception
    {
        public int StatusCode { get; }
        public string Path { get; }
        public string Details { get; }

        public SchemaRegistryException(string message, int statusCode, string path, string details = null)
            : base(message)
        {
            StatusCode = statusCode;
            Path = path;
            Details = details;
        }
    }
}