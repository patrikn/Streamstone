using System.Collections;
using System.Collections.Generic;

namespace Streamstone
{
    sealed class EventEntity : TableEntity
    {
        public const string RowKeyPrefix = "SS-SE-";

        public EventEntity()
        {
            EventProperties = EventProperties.None;
        }

        public EventEntity(Partition partition, RecordedEvent @event)
        {
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventVersionRowKey(@event.Version);
            EventProperties = @event.Properties;
            Version = @event.Version;
        }

        public int Version                  { get; set; }
        public override PropertyMap Properties { get => EventProperties; set => EventProperties = EventProperties.From(value); }

        protected override IDictionary<string, EntityProperty> WriteCustom(IDictionary<string, EntityProperty> withProperties)
        {
            withProperties["Version"] = Version;
            return withProperties;
        }

        protected override void ReadCustom(Dictionary<string, EntityProperty> properties)
        {
            Version = (int) properties["Version"].NumberValue();
        }

        public EventProperties EventProperties { get; private set; }
    }
}