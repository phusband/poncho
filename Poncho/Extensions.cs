using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Poncho.Extensions
{
    public static class Extensions
    {
        public static T ExecuteInTransaction<T>(this IDbConnection connection, IsolationLevel isolation, IDbCommand command, Func<IDbCommand, T> executor)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (command == null)
                throw new ArgumentNullException("command");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            using (var transaction = connection.BeginTransaction(isolation))
            {
                try
                {
                    command.Connection = connection;
                    command.Transaction = transaction;
                    T result = executor(command);
                    transaction.Commit();

                    return result;
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        public static T ExecuteInTransaction<T>(this IDbConnection connection, IsolationLevel isolation, IDbCommand command, Func<IDataReader, T> readFunction, CommandBehavior behavior = CommandBehavior.Default)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (command == null)
                throw new ArgumentNullException("command");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            using (var transaction = connection.BeginTransaction(isolation))
            {
                command.Connection = connection;
                command.Transaction = transaction;
                using (IDataReader reader = command.ExecuteReader(behavior))
                {
                    try
                    {
                        T result = readFunction(reader);
                        reader.Close(); // Reader must be closed before the transaction commits

                        transaction.Commit();
                        return result;
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }
        public static T ExecuteInTransaction<T>(this IDbConnection connection, Func<IDbTransaction, int?, T> function, IsolationLevel isolation = IsolationLevel.ReadCommitted, int? timeout = null)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            using (var transaction = connection.BeginTransaction(isolation))
            {
                try
                {
                    T result = function(transaction, timeout);
                    transaction.Commit();

                    return result;
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        public static T ExecuteInTransaction<T, P>(this IDbConnection connection, P param, Func<P, IDbTransaction, int?, T> function, IsolationLevel isolation = IsolationLevel.ReadCommitted, int? timeout = null)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            using (var transaction = connection.BeginTransaction(isolation))
            {
                try
                {
                    T result = function(param, transaction, timeout);
                    transaction.Commit();

                    return result;
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        public static T ExecuteInTransaction<T>(this IDbConnection connection, string commandText, object param, Func<string, object, IDbTransaction, int?, CommandType?, T> function,
            IsolationLevel isolation = IsolationLevel.ReadCommitted, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            bool wasClosed = (connection.State == ConnectionState.Closed);
            if (wasClosed)
                connection.Open();

            using (var transaction = connection.BeginTransaction(isolation))
            {
                try
                {
                    T result = function(commandText, param, transaction, timeout, commandType);
                    transaction.Commit();

                    if (wasClosed)
                        connection.Close();

                    return result;
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        public static T ExecuteInTransaction<T>(this IDbConnection connection, string commandText, object param, Func<string, object, IDbTransaction, bool, int?, CommandType?, T> function,
            bool buffered, IsolationLevel isolation = IsolationLevel.ReadCommitted, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            bool wasClosed = (connection.State == ConnectionState.Closed);
            if (wasClosed)
                connection.Open();

            using (var transaction = connection.BeginTransaction(isolation))
            {
                try
                {
                    T result = function(commandText, param, transaction, buffered, timeout, commandType);
                    transaction.Commit();

                    if (wasClosed)
                        connection.Close();

                    return result;
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }

        public static T Execute<T, P>(this IDbTransaction transaction, P param, Func<P, IDbTransaction, int?, T> function, int? timeout = null)
        {
            if (transaction == null)
                throw new ArgumentException("transaction cannot be null", "transaction");

            if (transaction.Connection == null)
                throw new InvalidOperationException("transaction has no available connection");

            var connection = transaction.Connection;
            bool wasClosed = (connection.State == ConnectionState.Closed);

            if (wasClosed)
                connection.Open();

            T result = function(param, transaction, timeout);

            if (wasClosed)
                connection.Close();

            return result;
        }
        public static T Execute<T>(this IDbTransaction transaction, string commandText, object param, Func<string, object, IDbTransaction, int?, CommandType?, T> function,
            int? timeout, CommandType commandType = CommandType.Text)
        {
            if (transaction == null)
                throw new ArgumentException("transaction cannot be null", "transaction");

            if (transaction.Connection == null)
                throw new InvalidOperationException("transaction has no available connection");

            var connection = transaction.Connection;
            bool wasClosed = (connection.State == ConnectionState.Closed);

            if (wasClosed)
                connection.Open();

            T result = function(commandText, param, transaction, timeout, commandType);

            if (wasClosed)
                connection.Close();

            return result;
        }
        public static T Execute<T>(this IDbTransaction transaction, string commandText, object param, Func<string, object, IDbTransaction, bool, int?, CommandType?, T> function,
            bool buffered, int? timeout, CommandType commandType = CommandType.Text)
        {
            if (transaction == null)
                throw new ArgumentException("transaction cannot be null", "transaction");

            if (transaction.Connection == null)
                throw new InvalidOperationException("transaction has no available connection");

            var connection = transaction.Connection;
            bool wasClosed = (connection.State == ConnectionState.Closed);

            if (wasClosed)
                connection.Open();

            T result = function(commandText, param, transaction, buffered, timeout, commandType);

            if (wasClosed)
                connection.Close();

            return result;
        }

        public static DataTable ToDataTable<T>(this IEnumerable<T> entities) where T : class
        {
            Type entityType = typeof(T);
            DataTable table = new DataTable(entityType.Name);

            var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var propertyMethods = properties.Select(p => (Func<T, object>)Extensions.GetGetter(p)).ToArray();

            table.Columns.AddRange(properties.Select(p => new DataColumn(p.Name, p.PropertyType.BaseType())).ToArray());

            object[] values = new object[properties.Length];
            foreach (T entity in entities)
            {
                for (int i = 0; i < properties.Length; i++)
                    values[i] = propertyMethods[i](entity);

                table.Rows.Add(values);
            }

            return table;
        }
        public static Type BaseType(this Type type)
        {
            if (type != null && type.IsValueType && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return Nullable.GetUnderlyingType(type);

            return type;
        }

        internal static Func<object, object> GetGetter(PropertyInfo property)
        {
            MethodInfo method = property.GetGetMethod(true);
            MethodInfo genericHelper = typeof(Extensions).GetMethod("GetGetterImpl", BindingFlags.Static | BindingFlags.NonPublic);
            MethodInfo constructedHelper = genericHelper.MakeGenericMethod(method.DeclaringType, method.ReturnType);

            return (Func<object, object>)constructedHelper.Invoke(null, new object[] { method });
        }
        private static Func<object, object> GetGetterImpl<TTarget, TResult>(MethodInfo method) where TTarget : class
        {
            Func<TTarget, TResult> func = (Func<TTarget, TResult>)Delegate.CreateDelegate(typeof(Func<TTarget, TResult>), method);
            return (object target) => (TResult)func((TTarget)target);
        }
    }
}


