using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;


namespace Streamstone
{
    public sealed partial class Stream
    {
        class ProvisionOperation
        {
            readonly Stream stream;
            readonly ITable table;

            public ProvisionOperation(Stream stream)
            {
                Debug.Assert(stream.IsTransient);

                this.stream = stream;
                table = stream.Partition.Table;
            }

            public async Task<Stream> ExecuteAsync()
            {
                var insert = new Insert(stream);

                await table.ExecuteAsync(insert.Prepare()).ConfigureAwait(false);

                return insert.Result();
            }

            class Insert
            {
                readonly StreamEntity stream;
                readonly Partition partition;

                public Insert(Stream stream)
                {
                    this.stream = stream.Entity();
                    partition = stream.Partition;
                }

                public TableOperation Prepare() => TableOperation.Insert(stream);

                internal Stream Result() => From(partition, stream);
            }
        }

        class WriteOperation
        {
            readonly int MaxOperationsPerChunk;

            readonly Stream stream;
            readonly StreamWriteOptions options;
            readonly ITable table;
            readonly IEnumerable<RecordedEvent> events;

            public WriteOperation(Stream stream, StreamWriteOptions options, IEnumerable<EventData> events)
            {
                this.stream = stream;
                this.options = options;
                this.events = stream.Record(events);
                table = stream.Partition.Table;
                MaxOperationsPerChunk = table.MaxOperationsPerChunk;
            }

            public async Task<StreamWriteResult> ExecuteAsync()
            {
                var current = stream;

                foreach (var chunk in Chunks())
                {
                    var batch = chunk.ToBatch(current, options);

                    await table.ExecuteBatchAsync(batch.Prepare()).ConfigureAwait(false);

                    current = batch.Result();
                }

                return new StreamWriteResult(current, events.ToArray());
            }

            IEnumerable<Chunk> Chunks() => Chunk.Split(events, MaxOperationsPerChunk).Where(s => !s.IsEmpty);

            class Chunk
            {
                readonly int maxOperationsPerChunk;

                public static IEnumerable<Chunk> Split(IEnumerable<RecordedEvent> events, int maxOperationsPerChunk)
                {
                    var current = new Chunk(maxOperationsPerChunk);

                    foreach (var @event in events)
                    {
                        var next = current.Add(@event);

                        if (next != current)
                            yield return current;

                        current = next;
                    }

                    yield return current;
                }

                readonly List<RecordedEvent> events = new List<RecordedEvent>();
                int operations;

                Chunk(int maxOperationsPerChunk)
                {
                    this.maxOperationsPerChunk = maxOperationsPerChunk;
                }

                Chunk(RecordedEvent first, int maxOperationsPerChunk) : this(maxOperationsPerChunk) => Accomodate(first);

                Chunk Add(RecordedEvent @event)
                {
                    if (@event.Operations > maxOperationsPerChunk)
                        throw new InvalidOperationException(
                            string.Format("{0} include(s) in event {1}:{{{2}}}, plus event entity itself, is over Azure's max batch size limit [{3}]",
                                @event.IncludedOperations.Length, @event.Version, @event.Id, maxOperationsPerChunk));

                    if (!CanAccomodate(@event))
                        return new Chunk(@event, maxOperationsPerChunk);

                    Accomodate(@event);
                    return this;
                }

                void Accomodate(RecordedEvent @event)
                {
                    operations += @event.Operations;
                    events.Add(@event);
                }

                bool CanAccomodate(RecordedEvent @event)
                {
                    return operations + @event.Operations <= maxOperationsPerChunk;
                }

                public bool IsEmpty => events.Count == 0;

                public Batch ToBatch(Stream stream, StreamWriteOptions options)
                {
                    var entity = stream.Entity();
                    entity.Version += events.Count;
                    return new Batch(entity, events, options);
                }
            }

            class Batch
            {
                readonly List<EntityOperation> operations =
                    new List<EntityOperation>();

                readonly StreamEntity stream;
                readonly List<RecordedEvent> events;
                readonly StreamWriteOptions options;
                readonly Partition partition;

                internal Batch(StreamEntity stream, List<RecordedEvent> events, StreamWriteOptions options)
                {
                    this.stream = stream;
                    this.events = events;
                    this.options = options;
                    partition = stream.Partition;
                }

                internal TableBatchOperation Prepare()
                {
                    WriteStream();
                    WriteEvents();
                    WriteIncludes();

                    return ToBatch();
                }

                void WriteStream() => operations.Add(stream.Operation());

                void WriteEvents() => operations.AddRange(events.SelectMany(e => e.EventOperations));

                void WriteIncludes()
                {
                    if (!options.TrackChanges)
                    {
                        operations.AddRange(events.SelectMany(x => x.IncludedOperations));
                        return;
                    }

                    var tracker = new EntityChangeTracker();

                    foreach (var @event in events)
                        tracker.Record(@event.IncludedOperations);

                    operations.AddRange(tracker.Compute());
                }

                TableBatchOperation ToBatch()
                {
                    var result = new TableBatchOperation();

                    foreach (var each in operations)
                        result.Add(each);

                    return result;
                }

                internal Stream Result()
                {
                    return From(partition, stream);
                }
            }
        }

        class SetPropertiesOperation
        {
            readonly Stream stream;
            readonly ITable table;
            readonly StreamProperties properties;

            public SetPropertiesOperation(Stream stream, StreamProperties properties)
            {
                this.stream = stream;
                this.properties = properties;
                table = stream.Partition.Table;
            }

            public async Task<Stream> ExecuteAsync()
            {
                var replace = new Replace(stream, properties);

                await table.ExecuteAsync(replace.Prepare()).ConfigureAwait(false);

                return replace.Result();
            }

            class Replace
            {
                readonly StreamEntity stream;
                readonly Partition partition;

                public Replace(Stream stream, StreamProperties properties)
                {
                    this.stream = stream.Entity();
                    this.stream.Properties = properties;
                    partition = stream.Partition;
                }

                internal TableOperation Prepare()
                {
                    return TableOperation.Replace(stream);
                }

                internal Stream Result() => From(partition, stream);
            }
        }

        class OpenStreamOperation
        {
            readonly Partition partition;
            readonly ITable table;

            public OpenStreamOperation(Partition partition)
            {
                this.partition = partition;
                table = partition.Table;
            }

            public async Task<StreamOpenResult> ExecuteAsync() => Result(await table.ReadRowAsync<StreamEntity>(partition.PartitionKey, partition.StreamRowKey()));

            StreamOpenResult Result(StreamEntity entity)
            {
                return entity != null
                    ? new StreamOpenResult(true, From(partition, entity))
                    : StreamOpenResult.NotFound;
            }
        }

        class ReadOperation<T> where T : class, new()
        {
            readonly Partition partition;
            readonly ITable table;

            readonly int startVersion;
            readonly int sliceSize;

            public ReadOperation(Partition partition, int startVersion, int sliceSize)
            {
                this.partition = partition;
                this.startVersion = startVersion;
                this.sliceSize = sliceSize;
                table = partition.Table;
            }

            public async Task<StreamSlice<T>> ExecuteAsync() =>
                // ReSharper disable once PossibleNullReferenceException
                Result(await partition.ReadSlice<StreamEntity, T>(startVersion, sliceSize));

            StreamSlice<T> Result((StreamEntity, IEnumerable<T>) entities)
            {
                var (header, events) = entities;
                if (header == null)
                {
                    throw new StreamNotFoundException(partition.PartitionKey, partition.Table.Name, partition.Table.StorageUri);
                }
                var stream = BuildStream(header);

                return new StreamSlice<T>(stream, events.ToArray(), startVersion, sliceSize);
            }

            Stream BuildStream(StreamEntity entity) => From(partition, entity);
        }
    }
}
