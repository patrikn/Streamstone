using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos.Table;

using Streamstone;
using Streamstone.Utility;

namespace StreamStone
{
    public class AzureCloudTable : ITable
    {
        internal readonly CloudTable table;

        public AzureCloudTable(CloudTable table)
        {
            this.table = table;
        }

        public string Name => table.Name;
        public string StorageUri => table.StorageUri.ToString();
        public int MaxOperationsPerChunk => 99;

        public Task<TableResult> ExecuteAsync(TableOperation operation)
        {
            return table.ExecuteAsync(operation);
        }

        public Task ExecuteBatchAsync(TableBatchOperation operation)
        {
            return table.ExecuteBatchAsync(operation);
        }

        public IEnumerable<TEntity> RowKeyPrefixQuery<TEntity>(string partitionKey, string prefix) where TEntity : ITableEntity, new()
        {
            var filter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    WhereRowKeyPrefixFilter(prefix));

            var query = new TableQuery<TEntity>().Where(filter);
            TableContinuationToken token = null;
            do
            {
                var segment = table.ExecuteQuerySegmentedAsync(query, null).Result;
                token = segment.ContinuationToken;
                foreach (var res in segment.Results)
                    yield return res;
            }
            while (token != null);
        }

        public async Task<(THeader, IEnumerable<TEvent>)> ReadRows<THeader, TEvent>(
            string partitionKey,
            string headerRowKey,
            string rowKeyStart,
            string rowKeyEnd)
            where THeader : ITableEntity, new()
            where TEvent : new()
        {
            var query = new TableQuery<DynamicTableEntity>().Where(WhereRowKeyBetweenFilter(partitionKey, headerRowKey, rowKeyStart, rowKeyEnd));
            var result = new List<DynamicTableEntity>();
            TableContinuationToken token = null;

            do
            {
                var segment = await table.ExecuteQuerySegmentedAsync(query, token).ConfigureAwait(false);
                token = segment.ContinuationToken;
                result.AddRange(segment.Results);
            }
            while (token != null);

            var opCtx = new OperationContext();

            var streamEntity = ReadAndRemoveStreamEntity<THeader>(partitionKey, headerRowKey, result, opCtx);

            var events = result.Select(dte =>
            {
                var entity = new TEvent();
                if (entity is ITableEntity tableEntity)
                {
                    tableEntity.Timestamp = dte.Timestamp;
                    tableEntity.ETag = dte.ETag;
                    tableEntity.PartitionKey = dte.PartitionKey;
                    tableEntity.RowKey = dte.RowKey;
                    tableEntity.ReadEntity(dte.Properties, opCtx);
                }
                else
                {
                    TableEntity.ReadUserObject(entity, dte.Properties, opCtx);
                }
                return entity;
            });

            return (streamEntity, events);
        }

        THeader ReadAndRemoveStreamEntity<THeader>(string partitionKey, string headerRowKey, List<DynamicTableEntity> result, OperationContext opCtx)
            where THeader : ITableEntity, new()
        {
            var header = FindStreamEntity(result, partitionKey, headerRowKey);
            result.Remove(header);
            var streamEntity = new THeader();
            streamEntity.ReadEntity(header.WriteEntity(opCtx), opCtx);
            return streamEntity;
        }

        DynamicTableEntity FindStreamEntity(IEnumerable<DynamicTableEntity> entities, string partitionKey, string streamRowKey)
        {
            var result = entities.SingleOrDefault(x => x.RowKey == streamRowKey);

            if (result == null)
                throw new StreamNotFoundException(partitionKey, table.Name, table.StorageUri.ToString());

            return result;
        }

        public async Task<T> ReadRowAsync<T>(string partitionKey, string rowKey)
            where T : ITableEntity, new()
        {
            var filter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(nameof(StreamEntity.PartitionKey), QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(nameof(StreamEntity.RowKey), QueryComparisons.Equal, rowKey));

            var query = new TableQuery<T>().Where(filter);

            var segment = await table.ExecuteQuerySegmentedAsync(query, null);
            return segment.SingleOrDefault();
        }

        public async Task<List<T>> ReadPartition<T>(string partitionKey)
            where T: new()
        {
            var filter = TableQuery.GenerateFilterCondition(nameof(StreamEntity.PartitionKey), QueryComparisons.Equal, partitionKey);
            var query = new TableQuery<DynamicTableEntity>().Where(filter);

            var entities = new List<DynamicTableEntity>();
            TableContinuationToken token = null;

            do
            {
                var segment = await table.ExecuteQuerySegmentedAsync(query, token);
                token = segment.ContinuationToken;

                entities.AddRange(segment.Results);
            }
            while (token != null);

            var operationContext = new OperationContext();
            return entities.Select(dte =>
            {
                var item = new T();
                if (item is ITableEntity tableEntity)
                {
                    tableEntity.Timestamp = dte.Timestamp;
                    tableEntity.ETag = dte.ETag;
                    tableEntity.PartitionKey = dte.PartitionKey;
                    tableEntity.RowKey = dte.RowKey;
                    tableEntity.ReadEntity(dte.Properties, operationContext);
                }
                else
                {
                    TableEntity.ReadUserObject(item, dte.Properties, operationContext);
                }
                return item;
            }).ToList();

        }

        string WhereRowKeyBetweenFilter(string partitionKey, string streamRowKey, string rowKeyStart, string rowKeyEnd)
        {
            return TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.PartitionKey), QueryComparisons.Equal, partitionKey),
                TableOperators.And,
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.RowKey), QueryComparisons.Equal, streamRowKey),
                    TableOperators.Or,
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.RowKey), QueryComparisons.GreaterThanOrEqual, rowKeyStart),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.RowKey), QueryComparisons.LessThanOrEqual, rowKeyEnd)
                        )
                    )
                );
        }

        string WhereRowKeyPrefixFilter(string prefix)
        {
            var range = new PrefixRange(prefix);
            var filter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, range.Start),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, range.End));

            return filter;
        }
    }
}
