using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    public interface ITableEntity
    {
        string PartitionKey { get; set; }
        string RowKey { get; set; }
        string ETag { get; set; }
        PropertyMap Properties { get; set; }
        IDictionary<string, EntityProperty> WriteEntity();
        void ReadEntity(Dictionary<string,EntityProperty> properties);
    }

    public static class ITableEntityExt
    {
        public static IDictionary<string, EntityProperty> WithProperties(this ITableEntity entity, IDictionary<string, EntityProperty> dict)
        {
            return dict.Concat(entity.Properties).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }
    }
}
