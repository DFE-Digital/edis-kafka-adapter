using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dfe.Edis.Kafka.Logging;

namespace Dfe.Edis.Kafka.OAuth
{
    public interface IKafkaProducerOAuthTokenClient<TKey, TValue>
    {
        void RefreshProducerToken(IProducer<TKey, TValue> producer, string oauthConfig);
    }

    public class KafkaOAuthTokenClient<TKey, TValue> : IKafkaProducerOAuthTokenClient<TKey, TValue>
    {
        private readonly HttpClient _httpClient;
        private readonly KafkaBrokerConfiguration _configuration;
        private readonly IKafkaLogger _logger;

        internal KafkaOAuthTokenClient(
            HttpClient httpClient,
            KafkaBrokerConfiguration configuration,
            IKafkaLogger logger)
        {
            _httpClient = httpClient;
            _configuration = configuration;
            _logger = logger;

            var creds = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{configuration.Username}:{configuration.Password}"));
            _httpClient.BaseAddress = new Uri(configuration.MdsUrl, UriKind.Absolute);
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", creds);
        }
        internal KafkaOAuthTokenClient(
            KafkaBrokerConfiguration configuration,
            IKafkaLogger logger)
            :this(new HttpClient(), configuration, logger) // TODO: Maybe find a way to inject HTTP client to make use of HTTPClientFactory
        {
        }

        public void RefreshProducerToken(IProducer<TKey, TValue> producer, string oauthConfig)
        {
            try
            {
                _logger.Log(LogLevel.Debug, "Starting to refresh producer OAuth token");
                    
                var token = GetTokenAsync().Result;
                producer.OAuthBearerSetToken(token.AuthToken, token.ExpiresAt, _configuration.Username);
                
                _logger.Log(LogLevel.Debug, "Successfully set producer OAuth token");
            }
            catch (Exception ex)
            {
                _logger.Log(LogLevel.Error, $"Error refreshing producer token:{Environment.NewLine}{ex}");
                producer.OAuthBearerSetTokenFailure(ex.Message);
            }
        }

        private async Task<OAuthResult> GetTokenAsync()
        {
            _logger.Log(LogLevel.Info, $"Acquiring token from {_httpClient.BaseAddress}");
            var response = await _httpClient.GetAsync("/security/1.0/authenticate");
            _logger.Log(LogLevel.Debug, $"Response status code from MDS was {(int)response.StatusCode}");

            var content = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                var message = $"Failed to get token from MDS. Http status: {response.StatusCode}";
                if (!string.IsNullOrEmpty(message))
                {
                    message += $", Content:{Environment.NewLine}{content}";
                }
                throw new Exception(message);
            }

            var result = JsonSerializer.Deserialize<OAuthResult>(content);
            _logger.Log(LogLevel.Info, $"Received token of type {result.TokenType} which expires in {result.ExpiresIn}");
            _logger.Log(LogLevel.Debug, $"Auth token is {result.AuthToken}");

            return result;
        }
    }
}