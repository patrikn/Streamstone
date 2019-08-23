﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ExpectedObjects;
using NUnit.Framework;

using StreamStone;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Setting_stream_properties
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
        public async Task When_property_map_is_empty()
        {
            var properties = new Dictionary<string, EntityProperty>();

            var previous = await Stream.ProvisionAsync(partition);
            var current  = await Stream.SetPropertiesAsync(previous, StreamProperties.From(properties));

            Assert.That(current.ETag, Is.Not.EqualTo(previous.ETag));
            StreamProperties.From(properties).ToExpectedObject().ShouldEqual(current.Properties);
        }

        [Test]
        public async Task When_concurrency_conflict()
        {
            var stream = await Stream.ProvisionAsync(partition);
            partition.UpdateStreamEntity();

            Assert.ThrowsAsync<ConcurrencyConflictException>(
                async ()=> await Stream.SetPropertiesAsync(stream, StreamProperties.None));
        }

        [Test]
        public async Task When_set_successfully()
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"P1", new EntityProperty(42)},
                {"P2", new EntityProperty("42")}
            };

            var stream = await Stream.ProvisionAsync(partition, StreamProperties.From(properties));

            var newProperties = new Dictionary<string, EntityProperty>
            {
                {"P1", new EntityProperty(56)},
                {"P2", new EntityProperty("56")}
            };

            var newStream = await Stream.SetPropertiesAsync(stream, StreamProperties.From(newProperties));
            StreamProperties.From(newProperties).ToExpectedObject().ShouldEqual(newStream.Properties);

            var storedEntity = partition.RetrieveStreamEntity();
            var storedProperties = storedEntity.Properties;

            StreamProperties.From(newProperties).ToExpectedObject().ShouldEqual(storedProperties);
        }

        [Test]
        public void When_trying_to_set_properties_on_transient_stream()
        {
            var stream = new Stream(partition);

            partition.CaptureContents<EventEntity>(contents =>
            {
                Assert.ThrowsAsync<ArgumentException>(
                    async ()=> await Stream.SetPropertiesAsync(stream, StreamProperties.None));

                contents.AssertNothingChanged();
            });
        }
    }
}