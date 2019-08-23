using System;

using Newtonsoft.Json;

namespace Streamstone
{
    using Annotations;

    /// <summary>
    /// Represents errors thrown by Streamstone itself.
    /// </summary>
    public abstract class StreamstoneException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamstoneException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        [StringFormatMethod("message")]
        protected StreamstoneException(string message, params object[] args)
            : base(args.Length > 0 ? string.Format(message, args) : message)
        {}
    }

    /// <summary>
    /// This exception is thrown when opening stream that doesn't exist
    /// </summary>
    public sealed class StreamNotFoundException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        internal StreamNotFoundException(Partition partition)
            : base("Stream header was not found in partition '{1}' which resides in '{0}' table located at {2}",
                   partition.Table, partition, partition.Table.StorageUri)
        {
        }

        internal StreamNotFoundException(string partitionKey, string table, string storageUri)
            : base("Stream header was not found in partition '{1}' which resides in '{0}' table located at {2}",
                partitionKey, table, storageUri)
        {
        }
    }

    /// <summary>
    /// This exception is thrown when duplicate event is detected
    /// </summary>
    public sealed class DuplicateEventException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        public readonly Partition Partition;

        /// <summary>
        /// The id of duplicate event
        /// </summary>
        public readonly string Id;

        internal DuplicateEventException(Partition partition, string id)
        : this(partition.Table.Name, partition.PartitionKey, partition.Table.StorageUri, id)
        {
            Partition = partition;
            Id = id;
        }

        internal DuplicateEventException(string table, string partition, string storageUri, string id)
        : base("Found existing event with id '{3}' in partition '{1}' which resides in '{0}' table located at {2}",
            table, partition, storageUri, id)
        {
            Id = id;
            Partition = null;
        }
    }

    /// <summary>
    /// This exception is thrown when included entity operation has conflicts in a partition
    /// </summary>
    public sealed class IncludedOperationConflictException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        public readonly Partition Partition;

        /// <summary>
        /// The included entity
        /// </summary>
        public readonly ITableEntity Entity;

        IncludedOperationConflictException(Partition partition, ITableEntity entity, string message)
            : base(message)
        {
            Partition = partition;
            Entity = entity;
        }

        internal static IncludedOperationConflictException Create(Partition partition, EntityOperation include)
        {
            var table = partition.Table;
            var storageUri = table.StorageUri;
            var includeEntity = include.Entity;
            var dump = Dump(includeEntity);
            var typeName = include.GetType().Name;

            return Create(partition, table.Name, typeName, storageUri, dump, includeEntity);
        }

        internal static IncludedOperationConflictException Create(
            Partition partition,
            string tableName,
            string typeName,
            string storageUri,
            string dump,
            ITableEntity includeEntity)
        {
            var message = string.Format(
                "Included '{3}' operation had conflicts in partition '{1}' which resides in '{0}' table located at {2}\n" +
                    "Dump of conflicting [{5}] contents follows: \n\t{4}",
                tableName, partition, storageUri,
                typeName, dump, includeEntity.GetType());

            return new IncludedOperationConflictException(partition, includeEntity, message);
        }

        static string Dump(ITableEntity entity)
        {
            return JsonConvert.SerializeObject(entity);
        }
    }

    /// <summary>
    /// This exception is thrown when stream write/povision operation has conflicts in a partition
    /// </summary>
    public sealed class ConcurrencyConflictException : StreamstoneException
    {
        /// <summary>
        /// The target partition
        /// </summary>
        public readonly Partition Partition;

        internal ConcurrencyConflictException(Partition partition, string details)
            : base("Concurrent write detected for partition '{1}' which resides in table '{0}' located at {2}. See details below.\n{3}",
                   partition.Table, partition, partition.Table.StorageUri, details)
        {
            Partition = partition;
        }

        internal ConcurrencyConflictException(string table, string partition, string storageUri, string details)
            : base("Concurrent write detected for partition '{1}' which resides in table '{0}' located at {2}. See details below.\n{3}",
                table, partition, storageUri, details)
        {
            Partition = null;
        }

        internal static Exception EventVersionExists(Partition partition, int version)
        {
            return new ConcurrencyConflictException(partition, string.Format("Event with version '{0}' is already exists", version));
        }

        internal static Exception StreamChanged(Partition partition)
        {
            return new ConcurrencyConflictException(partition, "Stream header has been changed in a storage");
        }

        internal static Exception StreamChangedOrExists(Partition partition)
        {
            return new ConcurrencyConflictException(partition, "Stream header has been changed or already exists in a storage");
        }
    }

    /// <summary>
    /// This exception is thrown when Streamstone receives unexpected response from underlying WATS layer.
    /// </summary>
    public sealed class UnexpectedStorageResponseException : StreamstoneException
    {
        internal UnexpectedStorageResponseException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}