namespace Streamstone
{
    public class TableOperation
    {
        TableOperation(TableOperationType operationType, ITableEntity entity)
        {
            OperationType = operationType;
            Entity = entity;
        }

        public static TableOperation Replace(ITableEntity entity)
        {
            return new TableOperation(TableOperationType.Replace, entity);
        }

        public static TableOperation Insert(ITableEntity entity)
        {
            return new TableOperation(TableOperationType.Insert, entity);
        }

        public static TableOperation Delete(ITableEntity entity)
        {
            return new TableOperation(TableOperationType.Delete, entity);
        }

        public static TableOperation InsertOrMerge(ITableEntity entity)
        {
            return new TableOperation(TableOperationType.InsertOrMerge, entity);
        }

        public static TableOperation InsertOrReplace(ITableEntity entity)
        {
            return new TableOperation(TableOperationType.InsertOrReplace, entity);
        }

        public static TableOperation Merge(ITableEntity entity)
        {
            return new TableOperation(TableOperationType.Merge, entity);
        }

        public TableOperationType OperationType { get; set; }
        public ITableEntity Entity { get; set; }
    }

    public enum TableOperationType
    {
        Insert,
        Delete,
        Replace,
        InsertOrReplace,
        Merge,
        InsertOrMerge
    }
}