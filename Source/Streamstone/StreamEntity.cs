using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    sealed class StreamEntity : TableEntity
    {
        public const string FixedRowKey = "SS-HEAD";

        public StreamEntity()
        {
            Properties = StreamProperties.None;
        }

        public StreamEntity(Partition partition, string etag, int version, StreamProperties properties)
        {
            Partition = partition;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.StreamRowKey();
            ETag = etag;
            Version = version;
            Properties = properties;
        }

        public int Version { get; set; }

        public override PropertyMap Properties
        {
            get => StreamProperties;
            set => StreamProperties = StreamProperties.From(value);
        }

        public StreamProperties StreamProperties { get; private set; }

        protected override IDictionary<string, EntityProperty> WriteCustom(IDictionary<string, EntityProperty> withProperties)
        {
            withProperties["Version"] = Version;
            return withProperties;
        }

        protected override void ReadCustom(Dictionary<string, EntityProperty> properties)
        {
            Version = (int) properties["Version"].NumberValue();
        }

        public EntityOperation Operation()
        {
            var isTransient = ETag == null;

            return isTransient ? Insert() : ReplaceOrMerge();

            EntityOperation.Insert Insert() => new EntityOperation.Insert(this);

            EntityOperation ReplaceOrMerge() => ReferenceEquals(Properties, StreamProperties.None)
                ? new EntityOperation.UpdateMerge(this)
                : (EntityOperation) new EntityOperation.Replace(this);
        }

        [IgnoreProperty]
        public Partition Partition { get; set; }
    }
}
