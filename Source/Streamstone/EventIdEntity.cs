using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    class EventIdEntity : TableEntity
    {
        public const string RowKeyPrefix = "SS-UID-";

        public EventIdEntity()
        {}

        public EventIdEntity(Partition partition, RecordedEvent @event)
        {
            Event = @event;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventIdRowKey(@event.Id);
            Version = @event.Version;
        }

        public int Version
        {
            get; set;
        }

        public RecordedEvent Event
        {
            get; set;
        }

        protected override IDictionary<string, EntityProperty> WriteCustom(IDictionary<string, EntityProperty> withProperties)
        {
            withProperties["Version"] = Version;
            return withProperties;
        }

        protected override void ReadCustom(Dictionary<string, EntityProperty> properties)
        {
            Version = (int) properties["Version"].NumberValue();
        }
    }
}