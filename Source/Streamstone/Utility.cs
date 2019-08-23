using System.Collections.Generic;

namespace Streamstone
{
    namespace Utility
    {
        /// <summary>
        /// The set of helper extension methods for querying WATS tables
        /// </summary>
        public static class TableQueryExtensions
        {
            /// <summary>
            /// Query range of rows in partition having the same row key prefix
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="partition">The partition.</param>
            /// <param name="prefix">The row key prefix.</param>
            /// <returns>An instance of <see cref="IEnumerable{T}"/> that allow to scroll over all rows</returns>
            public static IEnumerable<TEntity> RowKeyPrefixQuery<TEntity>(this Partition partition, string prefix) where TEntity : ITableEntity, new()
            {
                return partition.Table.RowKeyPrefixQuery<TEntity>(partition.PartitionKey, prefix);
            }
        }
    }
}