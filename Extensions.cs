using System;
using System.Data;

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
    }
}


