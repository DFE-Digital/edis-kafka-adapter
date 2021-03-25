using System;
using System.Text.Json.Serialization;

namespace Dfe.Edis.Kafka.OAuth
{
    public class OAuthResult
    {
        private static readonly DateTime UnixEpoch = DateTime.SpecifyKind(new DateTime(1970, 1, 1), DateTimeKind.Utc);
        
        [JsonPropertyName("auth_token")]
        public string AuthToken { get; set; }
        
        [JsonPropertyName("token_type")]
        public string TokenType { get; set; }
        
        [JsonPropertyName("expires_in")]
        public long ExpiresIn { get; set; }

        public long ExpiresAt
        {
            get
            {
                return (long) (DateTime.UtcNow.AddMilliseconds(ExpiresIn) - UnixEpoch).TotalMilliseconds;
            }
        }
    }
}