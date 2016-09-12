using System;

namespace Poncho
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public string Name { get; set; }
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ExplicitKeyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class WriteAttribute : Attribute
    {
        private readonly bool _write;
        public bool Write { get { return _write; } }
        public WriteAttribute(bool write)
        {
            _write = write;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ComputedAttribute : Attribute
    {
    }
}
