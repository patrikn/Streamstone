using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    class TestStreamEntity : TableEntity
    {
        public DateTimeOffset Created   { get; set; }
        public bool Active              { get; set; }

        protected override IDictionary<string, EntityProperty> WriteCustom(IDictionary<string, EntityProperty> withProperties)
        {
            withProperties["Created"] = Created.ToUnixTimeMilliseconds();
            withProperties["Active"] = Active ? "true" : "false";
            return withProperties;
        }

        protected override void ReadCustom(Dictionary<string, EntityProperty> properties)
        {
            Active = properties["Active"].StringValue() == "true";
            Created = DateTimeOffset.FromUnixTimeMilliseconds((long) properties["Created"].NumberValue());
        }
    }

    class TestEventEntity : TableEntity
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

    class TestRecordedEventEntity : TestEventEntity
    {
        public int Version { get; set; }

        protected override IDictionary<string, EntityProperty> WriteCustom(IDictionary<string, EntityProperty> withProperties)
        {
            withProperties["Version"] = Version;
            return base.WriteCustom(withProperties);
        }

        protected override void ReadCustom(Dictionary<string, EntityProperty> properties)
        {
            base.ReadCustom(properties);
            Version = (int) properties["Version"].NumberValue();
        }
    }
}
