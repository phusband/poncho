using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Poncho.Extensions;
using Dapper;
using System.Reflection;
using System.Data;
using System.Data.Common;

namespace Poncho.Adapters
{
    internal sealed class SqlAdapter : DbAdapter
    {
        internal SqlAdapter(DbProviderFactory factory, DbConnection baseConnection, string connectionString)
            : base(factory, baseConnection, connectionString) { }

        public override int Insert<T>(T entity, IDbTransaction transaction = null)
        {
            bool isMultiple = (GetEnumerableElementType(entity) != null);
            if (isMultiple)
                return base.Insert(entity);

            string insertCommandText = GetInsertCommandText(entity);
            insertCommandText += ";SELECT SCOPE_IDENTITY() id";

            int id = 0;
            using (var connection = base.CreateConnection(true))
            using (transaction = connection.BeginTransaction(Isolation))
            {
                try
                {
                    using (var grid = connection.QueryMultiple(insertCommandText, entity, transaction, Timeout, CommandType.Text))
                    {
                        var first = grid.Read<IdWrapper>(false).FirstOrDefault();
                        if (first != null && first.id != null)
                        {
                            id = (int)first.id;
                            PropertyInfo[] keyProperties = KeyPropertiesCache(typeof(T)).ToArray();

                            // Update the id property on the inserted entity
                            if (keyProperties.Any())
                            {
                                var idProperty = keyProperties.First();
                                idProperty.SetValue(entity, Convert.ChangeType(id, idProperty.PropertyType), null);
                            }
                        }
                    }

                    transaction.Commit();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }

                return id;
            }
        }
    }
}
