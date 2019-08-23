using System;
using System.Collections.Generic;
using System.Linq;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.CredentialManagement;

using ExpectedObjects;

using StreamStone.DynamoDB;

namespace Streamstone
{
    using Utility;
    static class Storage
    {
        const string TableName = "Stream";

        public static ITable SetUp()
        {
            var dynamoDb = Environment.GetEnvironmentVariable("Streamstone-DynamoDB");
            if (dynamoDb != null)
            {
                var credentialChain = new CredentialProfileStoreChain();
                if (credentialChain.TryGetAWSCredentials("streamstone", out var cred))
                {
                    var client = new AmazonDynamoDBClient(cred, RegionEndpoint.EUCentral1);
                    try
                    {
                        client.DescribeTableAsync(TableName).Wait();
                    }
                    catch (Exception)
                    {
                        var keys = new List<KeySchemaElement>()
                        {
                            new KeySchemaElement("PartitionKey", KeyType.HASH),
                            new KeySchemaElement("RowKey", KeyType.RANGE)
                        };
                        var attributes = new List<AttributeDefinition>()
                        {
                            new AttributeDefinition("PartitionKey", ScalarAttributeType.S),
                            new AttributeDefinition("RowKey", ScalarAttributeType.S)
                        };
                        var req = new CreateTableRequest()
                        {
                            TableName = TableName,
                            KeySchema = keys,
                            BillingMode = BillingMode.PAY_PER_REQUEST,
                            AttributeDefinitions = attributes
                        };
                        client.CreateTableAsync(req).Wait();
                    }
                    DescribeTableResponse tableStatus;
                    do
                    {
                        tableStatus = client.DescribeTableAsync(TableName).Result;
                    }
                    while (tableStatus.Table.TableStatus == TableStatus.CREATING);

                    AppDomain.CurrentDomain.ProcessExit += (s, e) =>
                    {
                        try
                        {
                            // client.DeleteTableAsync(TableName);
                        }
                        catch (Exception)
                        {
                            // ignored
                        }
                    };

                    return new DynamoDBCloudTable(client, TableName);
                }
                else
                {
                    throw new Exception("Profile not found");
                }
            }

            throw new Exception("DynamoDB not found");
        }

        public static StreamEntity InsertStreamEntity(this Partition partition, int version = 0)
        {
            var entity = new StreamEntity
            {
                PartitionKey = partition.PartitionKey,
                RowKey = Api.StreamRowKey,
                Version = version
            };

            partition.Table.ExecuteAsync(TableOperation.Insert(entity)).Wait();
            return entity;
        }

        public static StreamEntity UpdateStreamEntity(this Partition partition, int version = 0)
        {
            var entity = RetrieveStreamEntity(partition);
            entity.Version = version;

            partition.Table.ExecuteAsync(TableOperation.Replace(entity)).Wait();
            return entity;
        }

        public static StreamEntity RetrieveStreamEntity(this Partition partition)
        {
            return partition.Table.ReadRowAsync<StreamEntity>(partition.PartitionKey, Api.StreamRowKey).Result;
        }

        public static void InsertEventEntities(this Partition partition, params string[] ids)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var e = new EventEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = (i+1).FormatEventRowKey()
                };

                partition.Table.ExecuteAsync(TableOperation.Insert(e)).Wait();
            }
        }

        public static EventEntity[] RetrieveEventEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<EventEntity>(prefix: Api.EventRowKeyPrefix).ToArray();
        }

        public static void InsertEventIdEntities(this Partition partition, params string[] ids)
        {
            foreach (var id in ids)
            {
                var e = new EventIdEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = id.FormatEventIdRowKey(),
                };

                partition.Table.ExecuteAsync(TableOperation.Insert(e)).Wait();
            }
        }

        public static EventIdEntity[] RetrieveEventIdEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<EventIdEntity>(prefix: Api.EventIdRowKeyPrefix).ToArray();
        }

        public static List<T> RetrieveAll<T>(this Partition partition)
            where T : ITableEntity, new()
        {
            return partition.Table.ReadPartition<T>(partition.PartitionKey).Result;
        }

        public static PartitionContents<T> CaptureContents<T>(this Partition partition, Action<PartitionContents<T>> continueWith) where T : ITableEntity, new()
        {
            return new PartitionContents<T>(partition, continueWith);
        }

        public class PartitionContents<T> where T : ITableEntity, new()
        {
            readonly Partition partition;
            readonly List<T> captured;

            public PartitionContents(Partition partition, Action<PartitionContents<T>> continueWith)
            {
                this.partition = partition;

                captured = partition.RetrieveAll<T>();
                continueWith(this);
            }

            public void AssertNothingChanged()
            {
                var current = partition.RetrieveAll<T>();
                captured.ToExpectedObject().ShouldMatch(current);
            }
        }
    }
}