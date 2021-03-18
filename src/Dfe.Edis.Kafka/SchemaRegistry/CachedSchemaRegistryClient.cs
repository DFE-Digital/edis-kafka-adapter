using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dfe.Edis.Kafka.SchemaRegistry
{
    public class CachedSchemaRegistryClient : ISchemaRegistryClient
    {
        private readonly ISchemaRegistryClient _innerClient;
        private readonly KafkaSchemaRegistryConfiguration _configuration;
        private readonly ConcurrentDictionary<string, CacheItem<int[]>> _schemaVersionListCache;
        private readonly ConcurrentDictionary<string, CacheItem<SchemaDetails>> _schemaDetailsCache;

        public CachedSchemaRegistryClient(ISchemaRegistryClient innerClient, KafkaSchemaRegistryConfiguration configuration)
        {
            _innerClient = innerClient;
            _configuration = configuration;
            
            _schemaVersionListCache = new ConcurrentDictionary<string, CacheItem<int[]>>();
            _schemaDetailsCache = new ConcurrentDictionary<string, CacheItem<SchemaDetails>>();
        }
        
        public async Task<int[]> ListSchemaVersionsAsync(string subjectName, CancellationToken cancellationToken)
        {
            if (!_schemaVersionListCache.TryGetValue(subjectName, out var cacheItem) ||
                cacheItem.ExpiryTime < DateTime.Now)
            {
                var versions = await _innerClient.ListSchemaVersionsAsync(subjectName, cancellationToken);
                cacheItem = new CacheItem<int[]>
                {
                    Value = versions,
                    ExpiryTime = DateTime.Now.Add(_configuration.CacheTimeout),
                };
                if (cacheItem.Value != null && cacheItem.Value.Any())
                {
                    _schemaVersionListCache.AddOrUpdate(subjectName, cacheItem, (k, v) => cacheItem);
                }
            }

            return cacheItem.Value;
        }

        public async Task<SchemaDetails> GetSchemaAsync(string subjectName, int version, CancellationToken cancellationToken)
        {
            var key = $"{subjectName}::{version}";
            
            if (!_schemaDetailsCache.TryGetValue(key, out var cacheItem) ||
                cacheItem.ExpiryTime < DateTime.Now)
            {
                var details = await _innerClient.GetSchemaAsync(subjectName, version, cancellationToken);
                cacheItem = new CacheItem<SchemaDetails>
                {
                    Value = details,
                    ExpiryTime = DateTime.Now.Add(_configuration.CacheTimeout),
                };
                if (cacheItem.Value != null)
                {
                    _schemaDetailsCache.AddOrUpdate(key, cacheItem, (k, v) => cacheItem);
                }
            }

            return cacheItem.Value;
        }
        
        
        private class CacheItem<T>
        {
            public T Value { get; set; }
            public DateTime ExpiryTime { get; set; }
        }
    }
}