using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

using Streamstone;

namespace StreamStone.DynamoDB
{
    public class DynamoDBCloudTable : ITable
    {
        readonly AmazonDynamoDBClient client;
        readonly Table table;
        readonly DynamoDBContext context;

        public DynamoDBCloudTable(AmazonDynamoDBClient client, string tableName)
        {
            this.client = client;
            table = Table.LoadTable(client, tableName);
            context = new DynamoDBContext(client);
            context.ConverterCache[typeof(DateTimeOffset)] = new DateTimeOffsetConverter();
            Name = tableName;
            StorageUri = tableName;
        }

        public string Name { get; }
        public string StorageUri { get; }
        public int MaxOperationsPerChunk => 24;

        public Task ExecuteAsync(TableOperation operation)
        {
            try
            {
                switch (operation.OperationType)
                {
                    case TableOperationType.Insert:
                        return ExecuteInsertAsync(operation);
                    case TableOperationType.Delete:
                        return ExecuteDeleteAsync(operation);
                    case TableOperationType.Replace:
                        return ExecuteReplaceAsync(operation);
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (AmazonDynamoDBException e)
            {
                throw TranslateException(e, operation.Entity.PartitionKey);
            }
        }

        Exception TranslateException(AmazonDynamoDBException dynamoDbException, string partitionKey)
        {
            return new ConcurrencyConflictException(table.TableName, partitionKey, table.TableName,
                "Stream header has been changed or already exists in a storage");
        }

        async Task ExecuteReplaceAsync(TableOperation operation)
        {
            var document = ToDocument(operation);
            try
            {
                await table.PutItemAsync(document, new PutItemOperationConfig()
                {
                    ConditionalExpression = new Expression()
                    {
                        ExpressionStatement = "ETag = :etag",
                        ExpressionAttributeValues = new Dictionary<string, DynamoDBEntry>()
                        {
                            [":etag"] = operation.Entity.ETag
                        }
                    }
                });
                operation.Entity.ETag = document["ETag"];
            }
            catch (ConditionalCheckFailedException)
            {
                throw new ConcurrencyConflictException(table.TableName, operation.Entity.PartitionKey, table.TableName, "ETag didn't match");
            }

        }

        async Task ExecuteDeleteAsync(TableOperation operation)
        {
            await table.DeleteItemAsync(operation.Entity.PartitionKey, operation.Entity.RowKey);
        }

        async Task ExecuteInsertAsync(TableOperation operation)
        {
            var document = ToDocument(operation);
            try
            {
                await table.PutItemAsync(document, new PutItemOperationConfig()
                {
                    ConditionalExpression = new Expression()
                    {
                        ExpressionStatement = "attribute_not_exists(PartitionKey)",
                    }
                });

                UpdateEntityAfterWrite(operation.Entity, document);
            }
            catch (AmazonDynamoDBException e)
            {
                throw TranslateException(e, operation.Entity.PartitionKey);
            }
        }

        static void UpdateEntityAfterWrite(ITableEntity entity, Document document)
        {
            entity.ETag = document["ETag"];
        }

        Document ToDocument(TableOperation operation)
        {
            var attributes = operation.Entity.WriteEntity();
            var document = new Document(attributes.ToDictionary(kvp => kvp.Key, kvp => ToDynamoDBEntry(kvp.Value)));
            // FIXME: document["PartitionKey"] = operation.Entity.PartitionKey,
            // FIXME: document["RowKey"] = operation.Entity.RowKey,
            // New etag for this version
            document["ETag"] = Guid.NewGuid().ToString();

            return document;
        }

        DynamoDBEntry ToDynamoDBEntry(EntityProperty val)
        {
            switch (val.Type)
            {
                case EntityPropertyType.Binary:
                    return val.BinaryValue();
                case EntityPropertyType.Number:
                    return val.NumberValue();
                case EntityPropertyType.String:
                    return val.StringValue();
                case EntityPropertyType.Null:
                    return DynamoDBNull.Null;
                default:
                    throw new Exception($"Unsupported type {val.Type}");
            }
        }

        EntityProperty ToEntityProperty(DynamoDBEntry val)
        {
            var primitive = val.AsPrimitive();
            if (primitive != null)
            {
                switch (primitive.Type)
                {
                    case DynamoDBEntryType.String:
                        return (string) primitive;
                    case DynamoDBEntryType.Numeric:
                        return (decimal) primitive;
                    case DynamoDBEntryType.Binary:
                        return (byte[]) primitive;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            // Nulls are represented with instances of DynamoDBNull
            else if (val.AsDynamoDBNull() != null)
            {
                return EntityProperty.NULL;
            }
            else
            {
                throw new ArgumentException($"Unsupported DynamoDBEntry type {val}");
            }
        }

        public async Task ExecuteBatchAsync(TableBatchOperation operation)
        {
            var items = new List<TransactWriteItem>();
            var updateEntities = new List<(ITableEntity, Document)>();
            foreach (TableOperation op in operation)
            {
                var hasETagCondition = op.Entity.ETag != "*";
                string etagCondition = "";
                if (hasETagCondition)
                {
                    etagCondition = " and ETag = :ExistingETag";
                }
                var item = new TransactWriteItem();
                // ReSharper disable once SwitchStatementMissingSomeCases
                var document = ToDocument(op);
                switch (op.OperationType)
                {
                    case TableOperationType.Insert:
                        item.Put = new Put()
                        {
                            ConditionExpression = "attribute_not_exists(PartitionKey)",
                            Item = document.ToAttributeMap(),
                            TableName = table.TableName
                        };
                        break;
                    case TableOperationType.Replace:
                        item.Put = new Put()
                        {
                            Item = document.ToAttributeMap(),
                            TableName = table.TableName,
                            ConditionExpression = $"attribute_exists(PartitionKey){etagCondition}",
                            ExpressionAttributeValues = hasETagCondition
                                ? new Dictionary<string, AttributeValue>()
                                {
                                    [":ExistingETag"] = new AttributeValue(op.Entity.ETag)
                                }
                                : new Dictionary<string, AttributeValue>()
                        };
                        break;
                    case TableOperationType.InsertOrReplace:
                        item.Put = new Put()
                        {
                            Item = document.ToAttributeMap(),
                            TableName = table.TableName,
                        };
                        break;
                    case TableOperationType.Delete:
                        item.Delete = new Delete()
                        {
                            Key = ItemKey(op),
                            TableName = table.TableName
                        };
                        break;
                    case TableOperationType.Merge:
                        var expressionAttributeValues = ExpressionAttributeValues(document);
                        if (hasETagCondition)
                        {
                            expressionAttributeValues.Add(":ExistingETag", new AttributeValue(op.Entity.ETag));
                        }
                        item.Update = new Update()
                        {
                            ConditionExpression = $"attribute_exists(PartitionKey){etagCondition}",
                            Key = ItemKey(op),
                            TableName = table.TableName,
                            UpdateExpression = UpdateExpression(document),
                            ExpressionAttributeNames = ExpressionAttributeNames(document),
                            ExpressionAttributeValues = expressionAttributeValues
                        };
                        break;
                    case TableOperationType.InsertOrMerge:
                        item.Update = new Update()
                        {
                            Key = ItemKey(op),
                            TableName = table.TableName,
                            UpdateExpression = UpdateExpression(document),
                            ExpressionAttributeNames = ExpressionAttributeNames(document),
                            ExpressionAttributeValues = ExpressionAttributeValues(document)
                        };
                        break;
                    default:
                        throw new Exception($"Unsupported transactional operation {op.OperationType}");
                }
                items.Add(item);
                updateEntities.Add((op.Entity, document));
            }
            var request = new TransactWriteItemsRequest()
            {
                TransactItems = items
            };

            try
            {
                await client.TransactWriteItemsAsync(request);
            }
            catch (Exception e)
            {
                throw TranslateException(e, operation);
            }

            foreach (var (entity, document) in updateEntities)
            {
                UpdateEntityAfterWrite(entity, document);
            }
        }

        Exception TranslateException(Exception dynamoDbException, TableBatchOperation operations)
        {
            if (dynamoDbException is TransactionCanceledException)
            {
                var startIndex = dynamoDbException.Message.IndexOf("[", StringComparison.Ordinal);
                if (startIndex > 0)
                {
                    var items = dynamoDbException.Message.Substring(startIndex + 1, dynamoDbException.Message.Length - startIndex - 2);
                    var errors = items.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToArray();
                    var errIdx = Array.FindIndex(errors, e => e != "None");
                    if (errIdx >= 0)
                    {
                        var operation = operations[errIdx];
                        var entity = operation.Entity;
                        if (errIdx == 0)
                        {
                            if (errors[errIdx] == "ConditionalCheckFailed" || errors[errIdx] == "TransactionConflict")
                            {
                                return new ConcurrencyConflictException(table.TableName, entity.PartitionKey, table.TableName,
                                    "Stream header has been changed or already exists in a storage");
                            }
                        }
                        else
                        {
                            if (errors[errIdx] == "ConditionalCheckFailed" || errors[errIdx] == "TransactionConflict")
                            {
                                if (entity is EventIdEntity eie)
                                {
                                    return new DuplicateEventException(table.TableName, eie.PartitionKey, table.TableName, eie.Event.Id);
                                }
                                if (entity is EventEntity ee)
                                {
                                    return new ConcurrencyConflictException(table.TableName, ee.PartitionKey, table.TableName,
                                        "Event with version '{version}' is already exists");
                                }
                                return IncludedOperationConflictException.Create(
                                    null, table.TableName, entity.GetType().Name, table.TableName, "", entity);
                            }
                        }
                    }
                }
            }
            return new UnexpectedStorageResponseException("Failed to translate Dynamo DB exception", dynamoDbException);
        }

        Dictionary<string, AttributeValue> ExpressionAttributeValues(Document document)
        {
            var values = GetAttributesForUpdate(document)
                .ToDictionary(kvp => $":{kvp.Key}", kvp => kvp.Value);
            return values;
        }

        static IEnumerable<KeyValuePair<string, AttributeValue>> GetAttributesForUpdate(Document document)
        {
            return document.ToAttributeMap()
                .Where(kvp => kvp.Key != "PartitionKey" && kvp.Key != "RowKey" && !kvp.Value.NULL);
        }

        Dictionary<string, string> ExpressionAttributeNames(Document document)
        {
            var keys = GetAttributesForUpdate(document)
                .ToDictionary(kvp => $"#{kvp.Key}", kvp => kvp.Key);
            return keys;
        }

        string UpdateExpression(Document document)
        {
            var toSet = document.Where(kvp => !(kvp.Key == "PartitionKey" || kvp.Key == "RowKey" || kvp.Value == null || kvp.Value is DynamoDBNull)).ToArray();
            var buf = new StringBuilder();
            if (toSet.Any())
            {
                buf.Append("SET ");
                var sep = "";
                foreach (var kvp in toSet)
                {
                    buf.Append(sep);
                    buf.Append($"#{kvp.Key} = :{kvp.Key}");
                    sep = ", ";
                }
            }

            return buf.ToString();
        }

        static Dictionary<string, AttributeValue> ItemKey(TableOperation op)
        {
            return new Dictionary<string, AttributeValue>()
            {
                ["PartitionKey"] = new AttributeValue(op.Entity.PartitionKey),
                ["RowKey"] = new AttributeValue(op.Entity.RowKey)
            };
        }

        public IEnumerable<TEntity> RowKeyPrefixQuery<TEntity>(string partitionKey, string prefix) where TEntity : ITableEntity, new()
        {
            var query = table.Query(partitionKey, new QueryFilter("RowKey", QueryOperator.BeginsWith, prefix));
            do
            {
                var segment = query.GetNextSetAsync().Result;
                foreach (var res in segment)
                {
                    var entity = ReadEntity<TEntity>(res);
                    yield return entity;
                }
            }
            while (!query.IsDone);
        }

        TEntity ReadEntity<TEntity>(Document res) where TEntity : ITableEntity, new()
        {
            if (res == null)
            {
                return default;
            }
            var entity = new TEntity();
            entity.ReadEntity(res.ToDictionary(kvp => kvp.Key, kvp => ToEntityProperty(kvp.Value)));
            return entity;
        }

        public async Task<(THeader, IEnumerable<TEvent>)> ReadRows<THeader, TEvent>(string partitionKey, string headerRowKey, string rowKeyStart, string rowKeyEnd)
            where THeader : ITableEntity, new()
            where TEvent : new()
        {
            var headerPending = ReadRowAsync<THeader>(partitionKey, headerRowKey);

            var query = new QueryOperationConfig()
            {
                KeyExpression = new Expression()
                {
                    ExpressionStatement = "PartitionKey = :partitionKey and RowKey between :rowKeyStart and :rowKeyEnd",
                    ExpressionAttributeValues = new Dictionary<string, DynamoDBEntry>()
                    {
                        [":partitionKey"] = partitionKey,
                        [":rowKeyStart"] = rowKeyStart,
                        [":rowKeyEnd"] = rowKeyEnd
                    }
                }
            };
            var entities = await ReadEntitiesFromQuery<TEvent>(query);

            return (await headerPending, entities);
        }

        async Task<List<TEntity>> ReadEntitiesFromQuery<TEntity>(QueryOperationConfig queryConfig)
            where TEntity : new()
        {
            var query = table.Query(queryConfig);
            var entities = new List<TEntity>();
            do
            {
                var segment = await query.GetNextSetAsync();

                foreach (var res in segment)
                {
                    entities.Add(context.FromDocument<TEntity>(res));
                }
            }
            while (!query.IsDone);
            return entities;
        }

        public async Task<T> ReadRowAsync<T>(string partitionKey, string rowKey) where T : ITableEntity, new()
        {
            var item = await table.GetItemAsync(partitionKey, rowKey);
            return ReadEntity<T>(item);
        }

        public Task<List<T>> ReadPartition<T>(string partitionKey)
            where T : ITableEntity, new()
        {
            return ReadEntitiesFromQuery<T>(new QueryOperationConfig()
            {
                KeyExpression = new Expression()
                {
                    ExpressionStatement = "PartitionKey = :partitionKey",
                    ExpressionAttributeValues = new Dictionary<string, DynamoDBEntry>()
                    {
                        [":partitionKey"] = partitionKey
                    }
                }
            });
        }
    }

    class DateTimeOffsetConverter : IPropertyConverter
    {
        public DynamoDBEntry ToEntry(object value)
        {
            return ((DateTimeOffset) value).UtcDateTime.ToLongTimeString();
        }

        public object FromEntry(DynamoDBEntry entry)
        {
            return new DateTimeOffset(DateTime.Parse(entry.AsString()));
        }
    }
}
