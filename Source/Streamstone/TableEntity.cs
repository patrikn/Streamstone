using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    public abstract class TableEntity : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string ETag { get; set; }
        public virtual PropertyMap Properties { get; set; } = new PropertyMap();

        public virtual IDictionary<string, EntityProperty> WriteEntity()
        {
            var entity = new Dictionary<string, EntityProperty>()
            {
                ["ETag"] = ETag,
                ["PartitionKey"] = PartitionKey,
                ["RowKey"] = RowKey
            };
            return WriteCustom(this.WithProperties(entity));
        }

        protected abstract IDictionary<string, EntityProperty> WriteCustom(IDictionary<string, EntityProperty> withProperties);

        public void ReadEntity(Dictionary<string, EntityProperty> properties)
        {
            PartitionKey = properties["PartitionKey"].StringValue();
            RowKey = properties["RowKey"].StringValue();
            ETag = properties["ETag"].StringValue();
            Properties = PropertyMap.From(properties);
            ReadCustom(properties);
        }

        protected abstract void ReadCustom(Dictionary<string, EntityProperty> properties);
    }
}