﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Reading_from_stream
    {
        Partition partition;
        ITable table;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
            partition = new Partition(table, Guid.NewGuid().ToString());
        }

        [Test]
        public void When_start_version_is_less_than_1()
        {
            Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async ()=> await Stream.ReadAsync<TestEventEntity>(partition, 0));

            Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async ()=> await Stream.ReadAsync<TestEventEntity>(partition, -1));
        }

        [Test]
        public void When_stream_doesnt_exist()
        {
            Assert.ThrowsAsync<StreamNotFoundException>(async () => await Stream.ReadAsync<TestEventEntity>(partition));
        }

        [Test]
        public async Task When_stream_is_empty()
        {
            await Stream.ProvisionAsync(partition);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(0));
        }

        [Test]
        public async Task When_version_is_greater_than_current_version_of_stream()
        {
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition, events.Length + 1);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(0));
        }

        [Test]
        public async Task When_all_events_fit_to_single_slice()
        {
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));
        }

        [Test]
        public async Task When_all_events_do_not_fit_single_slice()
        {
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, sliceSize: 1);

            Assert.That(slice.IsEndOfStream, Is.False);
            Assert.That(slice.Events.Length, Is.EqualTo(1));
            Assert.That(slice.Events[0].Version, Is.EqualTo(1));

            slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, slice.Events.Last().Version + 1);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(1));
            Assert.That(slice.Events[0].Version, Is.EqualTo(2));
        }

        [Test, Explicit]
        public async Task When_slice_size_is_bigger_than_azure_storage_page_limit()
        {
            const int sizeOverTheAzureLimit = 1500;
            const int numberOfWriteBatches = 50;

            var stream = await Stream.ProvisionAsync(partition);

            foreach (var batch in Enumerable.Range(1, numberOfWriteBatches))
            {
                var events = Enumerable
                    .Range(1, sizeOverTheAzureLimit / numberOfWriteBatches)
                    .Select(i => CreateEvent(batch + "e" + i))
                    .ToArray();

                var result = await Stream.WriteAsync(stream, events);
                stream = result.Stream;
            }

            var slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, sliceSize: 1500);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(1500));
        }

        [Test]
        public async Task When_requested_result_is_EventProperties()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));

            var e = slice.Events[0];
            Assert.That(e["Type"].StringValue, Is.EqualTo("StreamChanged"));
            Assert.That(e["Data"].StringValue, Is.EqualTo("{}"));
        }

        [Test]
        public async Task When_requested_result_is_DynamicTableEntity()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<StreamEntity>(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));

            var e = slice.Events[0];
            AssertSystemProperties(e);

            Assert.That(e.Properties["Id"].StringValue, Is.EqualTo("e1"));
            Assert.That(e.Properties["Type"].StringValue, Is.EqualTo("StreamChanged"));
            Assert.That(e.Properties["Data"].StringValue, Is.EqualTo("{}"));
        }

        [Test]
        public async Task When_requested_result_is_custom_TableEntity()
        {
            EventData[] events = { CreateEvent("e1"), CreateEvent("e2") };
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<CustomTableEntity>(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));

            var e = slice.Events[0];
            AssertSystemProperties(e);

            Assert.That(e.Id, Is.EqualTo("e1"));
            Assert.That(e.Type, Is.EqualTo("StreamChanged"));
            Assert.That(e.Data, Is.EqualTo("{}"));
        }

        void AssertSystemProperties(ITableEntity e)
        {
            Assert.That(e.PartitionKey, Is.EqualTo(partition.Key));
            Assert.That(e.RowKey, Is.EqualTo(partition.EventVersionRowKey(1)));
            Assert.That(e.ETag, Is.Not.Null.Or.Empty);
        }

        class CustomTableEntity : TableEntity
        {
            public string Id   { get; set; }
            public string Type { get; set; }
            public string Data { get; set; }
            protected override IDictionary<string, EntityProperty> WriteCustom(IDictionary<string, EntityProperty> withProperties)
            {
                withProperties["Id"] = Id;
                withProperties["Type"] = Type;
                withProperties["Data"] = Data;
                return withProperties;
            }

            protected override void ReadCustom(Dictionary<string, EntityProperty> properties)
            {
                Id = properties["Id"].StringValue();
                Type = properties["Type"].StringValue();
                Data = properties["Data"].StringValue();
            }
        }

        static EventData CreateEvent(string id)
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Id",   new EntityProperty(id)},
                {"Type", new EntityProperty("StreamChanged")},
                {"Data", new EntityProperty("{}")}
            };

            return new EventData(EventId.From(id), EventProperties.From(properties));
        }
    }
}