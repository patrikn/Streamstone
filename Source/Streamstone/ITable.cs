using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Streamstone
{
    public interface ITable
    {
        string Name { get; }

        string StorageUri { get; }
        int MaxOperationsPerChunk { get; }

        Task ExecuteAsync(TableOperation operation);

        Task ExecuteBatchAsync(TableBatchOperation operation);

        IEnumerable<TEntity> RowKeyPrefixQuery<TEntity>(string partitionKey, string prefix) where TEntity : ITableEntity, new();

        Task<(THeader, IEnumerable<TEvent>)> ReadRows<THeader, TEvent>(string partitionKey, string headerRowKey, string rowKeyStart, string rowKeyEnd)
            where THeader : ITableEntity, new()
            where TEvent : ITableEntity, new();

        Task<T> ReadRowAsync<T>(string partitionKey, string rowKey)
            where T : ITableEntity, new();

        Task<List<T>> ReadPartition<T>(string partitionKey) where T : ITableEntity, new();
    }
}