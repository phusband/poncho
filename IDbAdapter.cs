using System;
using System.Collections.Generic;
using System.Data;

namespace Poncho
{
    public interface IDbAdapter
    {
        bool Delete<T>(T entity, IDbTransaction transaction = null) where T : class;
        bool DeleteAll<T>(IDbTransaction transaction = null) where T : class;
        int ExecuteNonQuery(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        IDataReader ExecuteReader(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        T ExecuteReader<T>(Func<IDataReader, T> readFunction, string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        object ExecuteScalar(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        T ExecuteScalar<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        T Fetch<T>(object id, IDbTransaction transaction = null) where T : class;
        IEnumerable<T> FetchAll<T>(IDbTransaction transaction = null) where T : class;
        DataSet FillDataSet(params string[] tableNames);
        DataTable FillDataTable(string tableName);
        int Insert<T>(T entity, IDbTransaction transaction = null) where T : class;
        IEnumerable<object> Query(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        IEnumerable<T> Query<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text, bool buffered = true);
        object QueryFirst(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        T QueryFirst<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        object QueryFirstOrDefault(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        T QueryFirstOrDefault<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text);
        bool Update<T>(T entity, IDbTransaction transaction = null) where T : class;
        int UpdateData(DataSet set);
        int UpdateData(DataTable table);
    }
}
