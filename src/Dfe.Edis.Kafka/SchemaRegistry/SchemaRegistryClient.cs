using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Dfe.Edis.Kafka.SchemaRegistry
{
    public interface ISchemaRegistryClient
    {
        Task<int[]> ListSchemaVersionsAsync(string subjectName, CancellationToken cancellationToken);
        Task<SchemaDetails> GetSchemaAsync(string subjectName, int version, CancellationToken cancellationToken);
    }

    public class SchemaRegistryClient : ISchemaRegistryClient
    {
        private readonly HttpClient _client;

        public SchemaRegistryClient(HttpClient client, KafkaSchemaRegistryConfiguration configuration)
        {
            _client = client;

            _client.BaseAddress = new Uri(configuration.BaseUrl);
            if (!string.IsNullOrEmpty(configuration.Username))
            {
                var auth = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{configuration.Username}:{configuration.Password}"));
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", auth);
            }
        }

        public async Task<int[]> ListSchemaVersionsAsync(string subjectName, CancellationToken cancellationToken)
        {
            var urlPath = $"/subjects/{subjectName}/versions";
            var response = await _client.GetAsync(urlPath, cancellationToken);
            var content = response.Content != null ? await response.Content.ReadAsStringAsync() : null;
            if (!response.IsSuccessStatusCode)
            {
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return new int[0];
                }
                
                throw new SchemaRegistryException($"Error listing versions of subject '{subjectName}'",
                    (int) response.StatusCode, urlPath, content);
            }

            var versions = string.IsNullOrEmpty(content)
                ? new int[0]
                : JsonSerializer.Deserialize<int[]>(content);
            return versions;
        }

        public async Task<SchemaDetails> GetSchemaAsync(string subjectName, int version, CancellationToken cancellationToken)
        {
            var urlPath = $"/subjects/{subjectName}/versions/{version}";
            var response = await _client.GetAsync(urlPath, cancellationToken);
            var content = response.Content != null ? await response.Content.ReadAsStringAsync() : null;
            if (!response.IsSuccessStatusCode)
            {
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }
                
                throw new SchemaRegistryException($"Error listing versions of subject '{subjectName}'",
                    (int) response.StatusCode, urlPath, content);
            }
            if (string.IsNullOrEmpty(content))
            {
                throw new SchemaRegistryException("Schema registry has returned no content in successful response. Please check schema registry",
                    (int) response.StatusCode, urlPath);
            }

            var details = JsonSerializer.Deserialize<SchemaDetails>(content, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            });
            return details;
        }
    }
}