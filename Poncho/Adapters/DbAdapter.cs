using Dapper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Poncho.Extensions;
using System.Collections.Concurrent;

namespace Poncho.Adapters
{
    public abstract class DbAdapter : DbProviderFactory, IDbAdapter, IDisposable
    {
        // id wrapper class for custom insert implementations
        protected sealed class IdWrapper
        {
            public int? id { get; set; }
        }

        #region Properties

        private static readonly Func<ICloneable, Object> CloneConnection = getCloneMethod();
        private static Func<DbConnection, DbProviderFactory> GetProviderFactory = _getProviderFactoryMethod();
        private static readonly RandomNumberGenerator _random = RandomNumberGenerator.Create();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ExplicitKeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> FetchQueries = new ConcurrentDictionary<RuntimeTypeHandle, string>();
        private static readonly ConcurrentDictionary<string, DbProviderFactory> Providers = new ConcurrentDictionary<string, DbProviderFactory>(StringComparer.OrdinalIgnoreCase);

        private readonly IList<DbConnection> _activeConnections = new List<DbConnection>();
        private readonly IList<DbTransaction> _activeTransactions = new List<DbTransaction>();
        private readonly DbConnection _baseConnection;
        private DbCommandBuilder _commandBuilder;
        private readonly object _connectionLock = new object();
        private readonly DbConnectionStringBuilder _connectionStringBuilder;
        private readonly DbProviderFactory _providerFactory;
        private char _identifierSeparator = ' ';
        private string _joinSeparator;
        private string _quotePrefix;
        private string _quoteSuffix;
        private string[] _reservedWords;
        private string[] _schemaTables;
        private readonly DataSourceInformation _sourceInformation;
        private readonly object _transactionLock = new object();

        public ICollection<DbConnection> ActiveConnections
        {
            get
            {
                lock (_connectionLock)
                {
                    return _activeConnections;
                }
            }
        }
        public ICollection<DbTransaction> ActiveTransactions
        {
            get
            {
                lock(_transactionLock)
                {
                    return _activeTransactions;
                }
            }
        }
        public virtual bool CanBulkInsert { get; } = false;
        public DbCommandBuilder CommandBuilder
        {
            get { return _commandBuilder ?? (_commandBuilder = _providerFactory.CreateCommandBuilder()); }
        }
        public DbConnectionStringBuilder ConnectionStringBuilder
        {
            get { return _connectionStringBuilder; }
        }
        public char IdentifierSeparator
        {
            get
            {
                if (_identifierSeparator == ' ')
                {
                    char separator = '.';
                    string s = SourceInformation.CompositeIdentifierSeparatorPattern;
                    if (!string.IsNullOrEmpty(s))
                    {
                        separator = s.Replace("\\", string.Empty)[0];
                    }

                    _identifierSeparator = separator;
                }

                return _identifierSeparator;
            }
        }
        public int InsertBatchSize { get; set; } = 10000;
        public IsolationLevel Isolation { get; set; }
        public string JoinSeparator
        {
            get
            {
                if (string.IsNullOrEmpty(_joinSeparator))
                    _joinSeparator = QuoteSuffix + IdentifierSeparator + QuotePrefix;

                return _joinSeparator;
            }
        }
        public ICollection<DbConnection> OpenConnections
        {
            get { return ActiveConnections.Where(c => c.State == ConnectionState.Open).ToList(); }
        }
        public string ParameterMarker
        {
            get { return SourceInformation.ParameterMarker; }
        }
        public DbProviderFactory ProviderFactory
        {
            get { return _providerFactory; }
        }
        public string QuotePrefix
        {
            get
            {
                if (string.IsNullOrEmpty(_quotePrefix))
                {
                    if (string.IsNullOrEmpty(CommandBuilder.QuotePrefix))
                        CommandBuilder.QuotePrefix = "\"";

                    _quotePrefix = CommandBuilder.QuotePrefix.Trim();
                }

                return _quotePrefix;
            }
        }
        public string QuoteSuffix
        {
            get
            {
                if (string.IsNullOrEmpty(_quoteSuffix))
                {
                    if (string.IsNullOrEmpty(CommandBuilder.QuoteSuffix))
                        CommandBuilder.QuoteSuffix = "\"";

                    _quoteSuffix = CommandBuilder.QuoteSuffix.Trim();
                }

                return _quoteSuffix;
            }
        }
        public ICollection<string> ReservedWords
        {
            get { return _reservedWords ?? (_reservedWords = _getReservedWords()); }
        }
        public ICollection<string> SchemaTables
        {
            get { return _schemaTables ?? (_schemaTables = _getSchemaTables()); }
        }
        public DataSourceInformation SourceInformation
        {
            get { return _sourceInformation; }
        }
        public int? Timeout { get; set; } = 15;
        
        #endregion

        #region Constructors

        internal static DbAdapter CreateAdapter(DbConnection baseConnection)
        {
            string provider = Path.GetFileNameWithoutExtension(baseConnection.GetType().FullName);
            var factory = getProviderFactory(baseConnection);

            return CreateAdapter(provider, factory, baseConnection, baseConnection.ConnectionString);
        }
        internal static DbAdapter CreateAdapter(string provider, string connectionString)
        {
            var factory = getProviderFactory(provider);

            return CreateAdapter(provider, factory, null, connectionString);
        }
        private static DbAdapter CreateAdapter(string provider, DbProviderFactory providerFactory, DbConnection baseConnection, string connectionString)
        {
            switch (provider)
            {
                case "System.Data.SqlClient":
                    return new SqlAdapter(providerFactory, baseConnection, connectionString);
                default: return new DefaultAdapter(providerFactory, baseConnection, connectionString);
            }
        }

        protected DbAdapter(DbProviderFactory factory, string connectionString)
            : this(factory, null, connectionString) { }
        protected DbAdapter(DbProviderFactory factory, DbConnection baseConnection, string connectionString)
        {
            if (factory == null)
                throw new ArgumentException("DbProvider factory cannot be null", "factory");

            _providerFactory = factory;
            _baseConnection = baseConnection;
            _connectionStringBuilder = _providerFactory.CreateConnectionStringBuilder() ?? new DbConnectionStringBuilder(true);
            _connectionStringBuilder.ConnectionString = connectionString;
            _sourceInformation = _getSourceInformation();
        }

        #endregion

        #region Methods

        private static DbProviderFactory getProviderFactory(DbConnection baseConnection)
        {
            DbProviderFactory providerFactory;
            string providerName = Path.GetFileNameWithoutExtension(baseConnection.GetType().FullName);

            if (!DbAdapter.Providers.TryGetValue(providerName, out providerFactory))
            {
                providerFactory = GetProviderFactory(baseConnection) ?? DbProviderFactories.GetFactory(providerName);
                if (providerFactory == null)
                    throw new InvalidOperationException($"DbProviderFactory not found for {baseConnection.ConnectionString} connection");

                Providers[providerName] =  providerFactory;
            }

            return providerFactory;
        }
        private static DbProviderFactory getProviderFactory(string providerName)
        {
            DbProviderFactory providerFactory;
            if (!Providers.TryGetValue(providerName, out providerFactory))
            {
                providerFactory = DbProviderFactories.GetFactory(providerName);
                if (providerFactory == null)
                    throw new InvalidOperationException($"DbProviderFactory not found for {providerName} provider");

                Providers[providerName] = providerFactory;
            }

            return providerFactory;
        }
        private static Func<ICloneable, Object> getCloneMethod()
        {
            var cloneType = typeof(ICloneable);
            var cloneMethod = cloneType.GetMethod("Clone");
            var instance = Expression.Parameter(cloneType, "instance");
            return Expression.Lambda<Func<ICloneable, Object>>(Expression.Call(instance, cloneMethod), instance).Compile();
        }
        private static Func<DbConnection, DbProviderFactory> _getProviderFactoryMethod()
        {
            var connectionType = typeof(DbConnection);
            var factoryMethod = connectionType.GetProperty("ProviderFactory", BindingFlags.Instance | BindingFlags.NonPublic).GetGetMethod(true);
            var instance = Expression.Parameter(connectionType, "instance");

            return Expression.Lambda<Func<DbConnection, DbProviderFactory>>(Expression.Call(instance, factoryMethod), instance).Compile();
        }
        private static DataTable _getSchema(DbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            return connection.GetSchema();
        }
        private static DataTable _getSchemaCollection(DbConnection connection, string collectionName)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            return connection.GetSchema(collectionName);
        }
        private static string[] _getUsers(DbConnection connection)
        {
            DataTable userSchema = _getSchemaCollection(connection, "Users");
            return userSchema.AsEnumerable().Select(r => r.Field<string>("UserName")).ToArray();
        }
        private static void GetTypeMap(DbConnection connection)
        {
            var typeSchema = _getSchemaCollection(connection, DbMetaDataCollectionNames.DataTypes);
            var columnSchema = _getSchemaCollection(connection, "Columns");

            return;
        }

        protected static PropertyInfo GetSingleKey<T>(string method)
        {
            var type = typeof(T);
            var keys = KeyPropertiesCache(type);
            var explicitKeys = ExplicitKeyPropertiesCache(type);
            var keyCount = keys.Count + explicitKeys.Count;
            if (keyCount > 1)
                throw new DataException($"{method}<T> only supports an entity with a single [Key] or [ExplicitKey] property");
            if (keyCount == 0)
                throw new DataException($"{method}<T> only supports an entity with a [Key] or an [ExplicitKey] property");

            return keys.Any() ? keys.First() : explicitKeys.First();
        }
        protected static List<PropertyInfo> ComputedPropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pi;
            if (ComputedProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi.ToList();
            }

            var computedProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ComputedAttribute)).ToList();

            ComputedProperties[type.TypeHandle] = computedProperties;
            return computedProperties;
        }
        protected static List<PropertyInfo> KeyPropertiesCache(Type type)
        {

            IEnumerable<PropertyInfo> pi;
            if (KeyProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi.ToList();
            }

            var allProperties = TypePropertiesCache(type);
            var keyProperties = allProperties.Where(p =>
            {
                return p.GetCustomAttributes(true).Any(a => a is KeyAttribute);
            }).ToList();

            if (keyProperties.Count == 0)
            {
                var idProp = allProperties.FirstOrDefault(p => p.Name.ToLower() == "id");
                if (idProp != null && !idProp.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute))
                {
                    keyProperties.Add(idProp);
                }
            }

            KeyProperties[type.TypeHandle] = keyProperties;
            return keyProperties;
        }
        protected static List<PropertyInfo> TypePropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pis;
            if (TypeProperties.TryGetValue(type.TypeHandle, out pis))
            {
                return pis.ToList();
            }

            var properties = type.GetProperties().Where(IsWriteable).ToArray();
            TypeProperties[type.TypeHandle] = properties;
            return properties.ToList();
        }
        protected static List<PropertyInfo> ExplicitKeyPropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pi;
            if (ExplicitKeyProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi.ToList();
            }

            var explicitKeyProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute)).ToList();

            ExplicitKeyProperties[type.TypeHandle] = explicitKeyProperties;
            return explicitKeyProperties;
        }
        protected static bool IsWriteable(PropertyInfo pi)
        {
            var attributes = pi.GetCustomAttributes(typeof(WriteAttribute), false).ToList();
            if (attributes.Count != 1) return true;

            var writeAttribute = (WriteAttribute)attributes[0];
            return writeAttribute.Write;
        }
        protected static Type GetEnumerableElementType<T>(T item)
        {
            var type = typeof(T);
            if (type.IsArray)
                return type.GetElementType();

            var enumerable = item as IEnumerable;
            if (enumerable == null)
                return null;

            Type[] interfaces = enumerable.GetType().GetInterfaces();
            Type elementType = (from i in interfaces
                                where i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                                select i.GetGenericArguments()[0]
                               ).FirstOrDefault();

            if (elementType == null || elementType == typeof(object))
            {
                object firstElement = enumerable.Cast<object>().FirstOrDefault();
                if (firstElement != null)
                    elementType = firstElement.GetType();
            }
            return elementType;
        }

        private void _connectionStateChange(object sender, StateChangeEventArgs e)
        {
            switch (e.CurrentState)
            {
                case ConnectionState.Open:
                    lock (_connectionLock)
                        _activeConnections.Add(sender as DbConnection);
                    break;
                case ConnectionState.Closed:
                case ConnectionState.Broken:
                    lock (_connectionLock)
                        _activeConnections.Remove(sender as DbConnection);
                    break;
            }
        }
        private string[] _getReservedWords()
        {
            using (var connection = CreateConnection(true))
            {
                DataTable wordSchema = _getSchemaCollection(connection, DbMetaDataCollectionNames.ReservedWords);
                var reservedWords = new List<string>(wordSchema.Rows.Count);
                return wordSchema.AsEnumerable().Select(r => r.Field<string>("ReservedWord")).ToArray();
            }
        }
        private string[] _getSchemaTables()
        {
            using (var connection = CreateConnection(true))
            {
                DataTable schema = _getSchemaCollection(connection, "Tables");
                return schema.AsEnumerable().Select(r => r.Field<string>("TABLE_NAME").Trim('\'')).ToArray();
            }
        }
        private DataSourceInformation _getSourceInformation()
        {
            using (var connection = CreateConnection(true))
            {
                DataTable infoTable = _getSchemaCollection(connection, DbMetaDataCollectionNames.DataSourceInformation);
                return new DataSourceInformation(infoTable);
            }
        }
       
        protected virtual string GetDeleteCommandText<T>(T entity)
        {
            var type = GetEnumerableElementType(entity) ?? typeof(T);
            var tableName = GetTableName(type);
            if (!TableExists(tableName))
                throw new KeyNotFoundException($"Table {tableName} does not exist in the current database.");

            var keyProperties = KeyPropertiesCache(type).ToList();
            var explicitKeyProperties = ExplicitKeyPropertiesCache(type);
            if (!keyProperties.Any() && !explicitKeyProperties.Any())
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            var sb = new StringBuilder();
            sb.Append($"DELETE FROM {tableName} WHERE ");

            for (var i = 0; i < keyProperties.Count; i++)
            {
                var property = keyProperties.ElementAt(i);
                sb.Append(GetParameterAssignment(property.Name));
                if (i < keyProperties.Count - 1)
                    sb.Append(" AND ");
            }

            return sb.ToString();
        }
        protected virtual string GetFetchCommandText<T>(Type type, bool fetchAll = false)
        {
            string fetchCommandText;
            if (fetchAll)
            {
                var cacheType = typeof(List<T>);
                if (!FetchQueries.TryGetValue(cacheType.TypeHandle, out fetchCommandText))
                {
                    GetSingleKey<T>("FetchAll");
                    var tableName = GetTableName(type);

                    fetchCommandText = $"SELECT * FROM {tableName}";
                    FetchQueries[cacheType.TypeHandle] = fetchCommandText;
                }
            }
            else
            {
                if (!FetchQueries.TryGetValue(type.TypeHandle, out fetchCommandText))
                {
                    var key = GetSingleKey<T>("Fetch");
                    string tableName = GetTableName(type);
                    string paramAssignment = GetParameterAssignment(key.Name);

                    fetchCommandText = $"SELECT * FROM {tableName} WHERE {paramAssignment}";
                    FetchQueries[type.TypeHandle] = fetchCommandText;
                }
            }

            return fetchCommandText;
        }
        protected virtual string GetInsertCommandText<T>(T entity)
        {
            var type = GetEnumerableElementType(entity) ?? typeof(T);
            var tableName = GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed.ElementAt(i);
                sbColumnList.Append(Format(property.Name));
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbColumnList.Append(", ");
            }

            var sbParameterList = new StringBuilder(null);
            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed.ElementAt(i);
                sbParameterList.Append(GetParameterName(property.Name));
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbParameterList.Append(", ");
            }

            return $"INSERT INTO {tableName} ({sbColumnList.ToString()}) VALUES ({sbParameterList.ToString()})";
        }
        protected virtual string GetUpdateCommandText<T>(T entity)
        {
            var type = GetEnumerableElementType(entity) ?? typeof(T);
            var keyProperties = KeyPropertiesCache(type).ToList();
            var explicitKeyProperties = ExplicitKeyPropertiesCache(type);

            if (!keyProperties.Any() && !explicitKeyProperties.Any())
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            var tableName = GetTableName(type);

            var sb = new StringBuilder();
            sb.Append($"UPDATE {tableName} SET ");

            var allProperties = TypePropertiesCache(type);
            keyProperties.AddRange(explicitKeyProperties);
            var computedProperties = ComputedPropertiesCache(type);
            var nonIdProps = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            for (var i = 0; i < nonIdProps.Count; i++)
            {
                var property = nonIdProps.ElementAt(i);
                sb.Append(GetParameterAssignment(property.Name));
                if (i < nonIdProps.Count - 1)
                    sb.AppendFormat(", ");
            }
            sb.Append(" WHERE ");
            for (var i = 0; i < keyProperties.Count; i++)
            {
                var property = keyProperties.ElementAt(i);
                sb.Append(GetParameterAssignment(property.Name));
                if (i < keyProperties.Count - 1)
                    sb.Append(" AND ");
            }

            return sb.ToString();
        }

        public virtual string FormatCommandText(string commandText)
        {
            return commandText;
        }
        public string FormatCommandText(string prefix, string commandText)
        {
            string paramFormat = SourceInformation.ParameterMarker;
            string paramMarker = string.Format(paramFormat, string.Empty);
            if (paramMarker == "?")
                paramMarker = prefix;

            if (!string.IsNullOrEmpty(paramMarker) && paramMarker != prefix)
                commandText = commandText.Replace(prefix, paramMarker);

            return commandText;
        }
        public string GetParameterAssignment(string columnName)
        {
            return $"{QuotePrefix}{columnName}{QuoteSuffix} = {GetParameterName(columnName)}";
        }
        public string GetParameterIdentifier(DbParameter parameter)
        {
            if (SourceInformation.ParameterMarkerFormat == "?")
                return "?";

            string paramMarker = SourceInformation.NamedParameterMarker;
            if (paramMarker.Length != 1)
            {
                paramMarker = SourceInformation.ParameterMarkerPattern.Substring(SourceInformation.ParameterMarkerPattern.IndexOf('[') - 1, 1);
                SourceInformation.NamedParameterMarker = paramMarker;
            }

            return paramMarker + parameter.ParameterName;
        }
        public string GetParameterName(string parameterName)
        {
            int len = SourceInformation.ParameterNameMaxLength;
            string p = parameterName ?? string.Empty;

            if (len < 1)
                return SourceInformation.ParameterMarker;

            if (len < p.Length)
                p = p.Substring(0, len - 1);

            var reg = SourceInformation.ParameterNamePatternRegex;
            if (!reg.IsMatch(p))
                p = GenerateRandomParameterName(len);

            if (!p.StartsWith(SourceInformation.ParameterMarker))
                p = SourceInformation.ParameterMarker + p;

            return p;
        }
        public string GenerateRandomParameterName(int length)
        {
            length = (length == 0 || length > 8)
                ? 8 : length;

            var buffer = new byte[length];
            _random.GetBytes(buffer);

            var sb = new StringBuilder();
            int i = 0;

            foreach (var b in buffer)
            {
                var valid = b > 64 && b < 91; // A-Z are valid
                valid |= b > 96 && b < 123;   // a-z are also valid
                if (i > 0)
                {
                    valid |= b > 47 && b < 58;
                    // 0-9 are only valid if not the first char
                }
                var c = !valid ? (char)((b % 26) + 'a') : (char)b;

                sb.Append(c);
                i++;
            }
            return sb.ToString();
        }
        public string GetTableName(Type type)
        {
            return Format(type.Name);
        }
        public string Format(string itemName)
        {
            if (string.IsNullOrEmpty(itemName))
                return itemName;

            if (itemName.Contains(QuotePrefix) || itemName.Contains(QuoteSuffix))
                itemName = Unformat(itemName);

            var items = itemName.Split(IdentifierSeparator);
            if (items.Length > 1)
                itemName = string.Join(JoinSeparator, items);

            return QuotePrefix + itemName + QuoteSuffix;
        }
        public bool TableExists(string name)
        {
            name = name.Replace(QuotePrefix, "");
            name = name.Replace(QuoteSuffix, "");

            if (name.Contains(IdentifierSeparator))
            {
                var parts = name.Split(IdentifierSeparator);
                if (parts.Length == 2)
                    name = parts[1];
            }

            return SchemaTables.Contains(name, StringComparer.Ordinal) ||
                   SchemaTables.Contains(name, StringComparer.OrdinalIgnoreCase);
        }
        public string Unformat(string itemName)
        {
            if (string.IsNullOrEmpty(itemName))
                return itemName;

            var items = itemName.Split(IdentifierSeparator);
            if (items.Length <= 1)
                return itemName;

            var list = new List<string>(items.Length);
            for (int i = 0; i < items.Length; i++)
            {
                string item = items[i];
                int len = item.Length;

                if (len > 2 && item.StartsWith(QuotePrefix) &&
                    item.EndsWith(QuoteSuffix))
                {
                    item = item.Substring(1, len - 2);
                }

                list.Add(item);
            }

            return string.Join(IdentifierSeparator.ToString(), list.ToArray());
        }

        #endregion

        #region DbProviderFactory

        public new DbCommand CreateCommand()
        {
            throw new NotImplementedException();
        }
        public DbCommand CreateCommand(string commandText, CommandType commandType = CommandType.Text)
        {
            var command = _providerFactory.CreateCommand();
            command.CommandText = commandText;
            command.CommandType = commandType;

            return command;
        }
        public DbCommand CreateCommand(DbTransaction transaction)
        {
            var command = _providerFactory.CreateCommand();
            command.Transaction = transaction;

            return command;
        }
        public DbCommand CreateCommand(DbTransaction transaction, string commandText, CommandType commandType = CommandType.Text)
        {
            var command = CreateCommand(commandText, commandType);
            command.Transaction = transaction;

            return command;
        }
        public new ObservableDbConnection CreateConnection()
        {
            return CreateConnection(false);
        }
        public ObservableDbConnection CreateConnection(bool open)
        {
            DbConnection connection = null;
            var clonableConnection = _baseConnection as ICloneable;

            if (clonableConnection != null)
            {
                // Clone the base connection since we may not have credentials 
                connection = (DbConnection)CloneConnection(clonableConnection);
            }
            else
            {
                connection = _providerFactory.CreateConnection();
                connection.ConnectionString = ConnectionStringBuilder.ConnectionString;
            }

            connection.StateChange += _connectionStateChange;

            if (open)
                connection.Open();

            return new ObservableDbConnection(this, connection);
        }
        public DbDataAdapter CreateDataAdapter(DbCommand selectCommand)
        {
            var adapter = _providerFactory.CreateDataAdapter();
            adapter.SelectCommand = selectCommand;

            return adapter;
        }
        public DbParameter CreateParameter(string name, object value = null)
        {
            var parameter = _providerFactory.CreateParameter();
            parameter.ParameterName = GetParameterName(name);
            parameter.Value = value ?? DBNull.Value;

            return parameter;
        }
        public DbParameter CreateParameter(string name, DbType type, object value = null)
        {
            var parameter = CreateParameter(name, value);
            parameter.DbType = type;

            return parameter;
        }

        #endregion

        #region IDbAdapter

        public ObservableDbTransaction BeginTransaction()
        {
            return BeginTransaction(this.Isolation);
        }
        public ObservableDbTransaction BeginTransaction(DbConnection connection)
        {
            return BeginTransaction(connection, this.Isolation);
        }
        public ObservableDbTransaction BeginTransaction(IsolationLevel isolation)
        {
            DbConnection connection = CreateConnection();
            var transaction = BeginTransaction(connection, isolation);
            transaction.Completed += (o, e) => { (o as IDbTransaction)?.Connection?.Dispose(); };

            return transaction;
        }
        public ObservableDbTransaction BeginTransaction(DbConnection connection, IsolationLevel isolation)
        {
            return new ObservableDbTransaction(connection, isolation);
        }

        public virtual long BulkInsert<T>(IEnumerable<T> entities, IDbTransaction transaction = null) where T : class
        {
            if (transaction != null && transaction.Connection != null)
                return BulkInsertImpl(entities, transaction.Connection, transaction);

            using (var connection = CreateConnection(true))
                return BulkInsertImpl(entities, connection, connection.BeginTransaction(Isolation));
        }
        public virtual bool Delete<T>(T entity, IDbTransaction transaction = null) where T : class
        {
            if (entity == null)
                throw new ArgumentException("Cannot delete null object", entity.GetType().Name);

            string deleteCommandText = GetDeleteCommandText(entity);
            return ExecuteImpl(deleteCommandText, entity, transaction, CommandType.Text) > 0;
        }
        public virtual bool DeleteAll<T>(IDbTransaction transaction = null) where T : class
        {
            var type = typeof(T);
            var tableName = GetTableName(type);
            var deleteCommandText = $"DELETE FROM {tableName}";

            return ExecuteImpl(deleteCommandText, null, transaction, CommandType.Text) > 0;
        }
        public virtual int ExecuteNonQuery(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteImpl(FormatCommandText(commandText), param, transaction, commandType);
        }
        public virtual IDataReader ExecuteReader(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteImpl<IDataReader>(FormatCommandText(commandText), param, (con) => { return con.ExecuteReader; }, transaction, commandType);
        }
        public T ExecuteReader<T>(Func<IDataReader, T> readFunction, string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            using (var reader = ExecuteReader(commandText, param, transaction, commandType))
                return readFunction(reader);
        }
        public object ExecuteScalar(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteScalar<object>(commandText, param, transaction, commandType);
        }
        public virtual T ExecuteScalar<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteImpl<T>(FormatCommandText(commandText), param, (con) => { return con.ExecuteScalar<T>; }, transaction, commandType);
        }
        public virtual T Fetch<T>(object id, IDbTransaction transaction = null) where T : class
        {
            if (id == null)
                throw new ArgumentException("id parameter cannot be null", "id");

            var type = typeof(T);
            if (type.IsInterface)
                throw new NotSupportedException("Interfaces are not currently supported.");

            var dynParms = new DynamicParameters();
            dynParms.Add(GetParameterName("id"), id, null, null, null);

            string fetchCommandText = GetFetchCommandText<T>(type);
            return ExecuteImpl<T>(fetchCommandText, dynParms, (con) => { return con.QueryFirstOrDefault<T>; }, transaction, CommandType.Text);
        }
        public virtual IEnumerable<T> FetchAll<T>(IDbTransaction transaction = null) where T : class
        {
            var type = typeof(T);
            if (type.IsInterface)
                throw new NotSupportedException("Interfaces are not currently supported.");

            string fetchCommandText = GetFetchCommandText<T>(type, true);
            return QueryImpl<T>(fetchCommandText, null, transaction);
        }
        public DataSet FillDataSet(params string[] tableNames)
        {
            throw new NotImplementedException();
        }
        public DataTable FillDataTable(string tableName)
        {
            throw new NotImplementedException();
        }
        public virtual int Insert<T>(T entity, IDbTransaction transaction = null) where T : class
        {
            if (entity == null)
                throw new ArgumentException("Cannot update null object", entity.GetType().Name);

            string insertCommandText = GetInsertCommandText(entity);
            return ExecuteImpl(insertCommandText, entity, transaction, CommandType.Text);
        }
        public IEnumerable<object> Query(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return Query<object>(commandText, param, transaction, commandType);
        }
        public virtual IEnumerable<T> Query<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text, bool buffered = true)
        {
            return QueryImpl<T>(FormatCommandText(commandText), param, transaction, commandType, buffered);
        }
        public object QueryFirst(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return QueryFirst<object>(commandText, param, transaction, commandType);
        }
        public virtual T QueryFirst<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteImpl<T>(FormatCommandText(commandText), param, (con) => { return con.QueryFirst<T>; }, transaction, commandType);
        }
        public object QueryFirstOrDefault(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return QueryFirstOrDefault<object>(commandText, param, transaction, commandType);
        }
        public virtual T QueryFirstOrDefault<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteImpl<T>(FormatCommandText(commandText), param, (con) => { return con.QueryFirstOrDefault<T>; }, transaction, commandType);
        }
        public virtual bool Update<T>(T entity, IDbTransaction transaction = null) where T : class
        {
            if (entity == null)
                throw new ArgumentException("Cannot update null object", entity.GetType().Name);

            string updateCommandText = GetUpdateCommandText(entity);
            return ExecuteImpl(updateCommandText, entity, transaction, CommandType.Text) > 0;
        }
        public int UpdateData(DataSet set)
        {
            throw new NotImplementedException();
        }
        public int UpdateData(DataTable table)
        {
            throw new NotImplementedException();
        }

        protected virtual long BulkInsertImpl<T>(IEnumerable<T> entities, IDbConnection connection, IDbTransaction transaction) where T : class
        {
            throw new NotSupportedException("The current adapter does not support bulk inserting.");
        }
        protected virtual int ExecuteImpl(string commandText, object param, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteImpl<int>(commandText, param, (con) => { return con.Execute; }, transaction, commandType);
        }
        protected virtual T ExecuteImpl<T>(string commandText, object param, Func<IDbConnection, Func<string, object, IDbTransaction, int?, CommandType?, T>> executor, IDbTransaction transaction = null, CommandType commandType = CommandType.Text)
        {
            if (transaction != null && transaction.Connection != null)
                return transaction.Execute(commandText, param, executor(transaction.Connection), Timeout, commandType);

            using (var connection = CreateConnection(true))
                return connection.ExecuteInTransaction(commandText, param, executor(connection), Isolation, Timeout, commandType);
        }
        protected virtual IEnumerable<T> QueryImpl<T>(string commandText, object param = null, IDbTransaction transaction = null, CommandType commandType = CommandType.Text, bool buffered = true)
        {
            if (transaction != null && transaction.Connection != null)
                return transaction.Execute(commandText, param, transaction.Connection.Query<T>, buffered, Timeout, commandType);

            using (var connection = CreateConnection(true))
                return connection.ExecuteInTransaction(commandText, param, connection.Query<T>, buffered, Isolation, Timeout, commandType);
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            _commandBuilder?.Dispose();

            if (_activeConnections != null)
            {
                foreach (var connection in _activeConnections)
                    connection?.Dispose();                    
            }
        }

        #endregion
    }
}
