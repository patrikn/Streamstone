using System;

namespace Streamstone
{
    public class EntityProperty
    {
        public object Value { get; }
        public EntityPropertyType Type { get; }
        public byte[] BinaryValue() => (byte[]) Value;
        public decimal NumberValue() => (decimal) Value;
        public string StringValue() => (string) Value;

        public static readonly EntityProperty NULL = new EntityProperty(null, EntityPropertyType.Null);

        public EntityProperty(object obj, EntityPropertyType type)
        {
            if (obj == null && type != EntityPropertyType.Null)
            {
                throw new ArgumentException("Null values must have type NULL");
            }
            Value = obj;
            Type = type;
        }

        public EntityProperty(byte[] bytes)
            : this(bytes, EntityPropertyType.Binary)
        {
        }

        public EntityProperty(decimal value1)
            : this(value1, EntityPropertyType.Number)
        {
        }

        public EntityProperty(string str)
            : this(str, EntityPropertyType.String)
        {
        }

        public static implicit operator EntityProperty(string str)
        {
            return str != null ? new EntityProperty(str, EntityPropertyType.String) : NULL;
        }

        public static implicit operator EntityProperty(decimal number)
        {
            return new EntityProperty(number, EntityPropertyType.Number);
        }

        public static implicit operator EntityProperty(byte[] binary)
        {
            return new EntityProperty(binary, EntityPropertyType.Binary);
        }

        public EntityProperty Clone()
        {
            return (EntityProperty) MemberwiseClone();
        }
    }

    public enum EntityPropertyType
    {
        String,
        Binary,
        Number,
        Null
    }
}