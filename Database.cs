using Poncho.Adapters;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;

namespace Poncho
{
    public static class Database
    {
        #region Properties

        private const string AceProvider = "Microsoft.ACE.OLEDB.12.0";
        private const string JetProvider = "Microsoft.Jet.OLEDB.4.0";
        private static readonly Dictionary<string, DbAdapter> Adapters = new Dictionary<string, DbAdapter>(StringComparer.OrdinalIgnoreCase);

        #endregion

        #region Methods

        private static IDbAdapter CreateAdapter(ConnectionStringSettings settings)
        {
            return CreateAdapter(settings.ProviderName, settings.ConnectionString);
        }
        public static IDbAdapter CreateAdapter(string connectionName)
        {
            if (string.IsNullOrEmpty(connectionName))
                throw new ArgumentNullException("connectionName");

            var connectionSettings = ConfigurationManager.ConnectionStrings[connectionName];
            if (connectionSettings == null)
                throw new KeyNotFoundException(connectionName + " connection string not found.");

            return CreateAdapter(connectionSettings);
        }
        public static IDbAdapter CreateAdapter(string provider, string connectionString)
        {
            if (string.IsNullOrEmpty(provider))
                throw new ArgumentNullException("provider");

            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");

            DbAdapter adapter;
            if (!Adapters.TryGetValue(provider, out adapter))
            {
                adapter = DbAdapter.CreateAdapter(provider, connectionString);
                if (adapter == null)
                    throw new InvalidOperationException(string.Format("DbAdapter not found for {0} provider", provider));

                Adapters.Add(provider, adapter);
            }

            return adapter;
        }
        public static IDbAdapter CreateAdapter(IDbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            DbAdapter adapter;
            string provider = Path.GetFileNameWithoutExtension(connection.GetType().FullName);
            if (!Adapters.TryGetValue(provider, out adapter))
            {
                adapter = DbAdapter.CreateAdapter((DbConnection)connection);
                if (adapter == null)
                    throw new InvalidOperationException(string.Format("DbAdapter not found for {0} connection", connection.ConnectionString));

                Adapters.Add(provider, adapter);
            }

            return adapter;
        }

        #region Access

        public static OleDbConnection GetAccessConnection(string filePath, string password = "")
        {
#if x64
            throw new NotSupportedException("Ace/Jet OleDb providers not supported in x64-bit mode");
#else
            var connectionFormat = "Provider={0};Data Source={1};{2};";
            var securityString = "Persist Security Info=False";

            if (!string.IsNullOrEmpty(password))
                securityString = "Jet OLEDB:Database Password=" + password;

            var connectionString = string.Format(connectionFormat, AceProvider, filePath, securityString);
            return new OleDbConnection(connectionString);
#endif

        }
        public static string[] GetAccessSchemaTables(string filePath, string password = "")
        {
            using (DbConnection connection = GetAccessConnection(filePath, password))
                return _getSchemaTables(connection).Where(t => !t.StartsWith("MSys")).ToArray();
        }
        public static DataTable GetAccessTable(string filePath, string tableName, string password = "")
        {
            return GetAccessTables(filePath, password, tableName).Tables[tableName];
        }
        public static DataTable GetAccessTable(OleDbConnection connection, string tableName)
        {
            return GetAccessTables(connection, tableName).Tables[tableName];
        }
        public static DataSet GetAccessTables(string filePath, string password, params string[] tableNames)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(filePath);

            if (!File.Exists(filePath))
                throw new FileNotFoundException("Access database file not found", filePath);

            using (var acConnection = GetAccessConnection(filePath, password))
                return GetAccessTables(acConnection, tableNames);
        }
        public static DataSet GetAccessTables(OleDbConnection connection, params string[] tableNames)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            // Get all tables if none were provided
            if (tableNames == null || tableNames.Length == 0)
                tableNames = _getSchemaTables(connection).Where(t => !t.StartsWith("MSys")).ToArray();

            // We know beforehand that Access will be an OleDbFactory
            return FillDataSet(OleDbFactory.Instance, connection, tableNames);
        }

        #endregion

        #region Excel

        public static OleDbConnection GetExcelConnection(string filePath, bool hasHeaders = true)
        {
            return GetExcelConnection(filePath, hasHeaders, true);
        }
        public static OleDbConnection GetExcelConnection(string filePath, bool hasHeaders, bool asText)
        {

#if x64
            throw new NotSupportedException("Ace/Jet OleDb providers not supported in x64-bit mode");
#else
            string exProperties = string.Empty;
            string tblHeaders = hasHeaders ? "HDR=YES;" : "HDR=No;";
            string mode = asText ? "IMEX=1" : string.Empty;

            switch (Path.GetExtension(filePath))
            {
                case ".xls":
                    exProperties = "Excel 8.0;";
                    break;
                case ".xlsx":
                    exProperties = "Excel 12.0 Xml;";
                    break;
                case ".xlsb":
                    exProperties = "Excel 12.0;";
                    break;
                case ".xlsm":
                    exProperties = "Excel 12.0 Macro;";
                    break;
                case ".xltm":
                    exProperties = "Excel 12.0;";
                    break;
                default:
                    throw new NotSupportedException("File format not supported");
            }

            var connectionFormat = "Provider={0};Data Source={1};Extended Properties=\"{2}{3}{4}\"";
            var connectionString = string.Format(connectionFormat, AceProvider, filePath, exProperties, tblHeaders, mode);

            return new OleDbConnection(connectionString);
#endif
        }
        public static string[] GetExcelSchemaTables(string filePath)
        {
            using (var connection = GetExcelConnection(filePath))
                return _getSchemaTables(connection).Select(t => t.TrimEnd('$')).ToArray();
        }
        public static DataTable GetExcelTable(string filePath, string tableName, bool hasHeaders = true)
        {
            return GetExcelTables(filePath, tableName).Tables[tableName];
        }
        public static DataTable GetExcelTable(OleDbConnection connection, string tableName)
        {
            return GetExcelTables(connection, tableName).Tables[tableName];
        }
        public static DataSet GetExcelTables(string filePath, params string[] tableNames)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(filePath);

            if (!File.Exists(filePath))
                throw new FileNotFoundException("Excel File Not Found", filePath);

#if x64
            return _getExcelTables_64(filePath, tableNames);
#else
            using (var xlConnection = GetExcelConnection(filePath))
                return GetExcelTables(xlConnection, tableNames);
#endif
        }
        public static DataSet GetExcelTables(OleDbConnection connection, params string[] tableNames)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            // Get all tables if none were provided
            if (tableNames == null || tableNames.Length == 0)
                tableNames = _getSchemaTables(connection);

            var fixedTableNames = tableNames.Select(t => string.Format
                                ("[{0}{1}]", t, t.EndsWith("$")
                                    ? ""
                                    : "$")
                                ).ToArray();

            DataSet excelSet = FillDataSet(OleDbFactory.Instance, connection, fixedTableNames);

            foreach (DataTable excelTable in excelSet.Tables)
                excelTable.TableName = excelTable.TableName
                    .Trim(new char[] { '[', ']' })
                    .TrimEnd(new char[] { '$' });

            return excelSet;
        }
#if x64
        private static DataSet _getExcelTables_64(string filePath, string[] tableNames, bool hasHeaders = true)
        {

            string ext = Path.GetExtension(filePath);
            bool isBinary;

            if (new string[] { ".xls", ".xlsb", ".xlt", ".xla" }.Any(x => x == ext))
            {
                isBinary = true;
            }
            else if (new string[] { ".xlsx", ".xlsm", ".xltx", ".xltm", ".xlam" }.Any(x => x == ext))
            {
                isBinary = false;
            }
            else
            {
                throw new NotSupportedException(string.Format("Filetype '{0}' not supported", ext));
            }

            using (var excelStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {

                Excel.IExcelDataReader excelReader = null;

                try
                {
                    if (isBinary)
                        excelReader = Excel.ExcelReaderFactory.CreateBinaryReader(excelStream);
                    else
                        excelReader = Excel.ExcelReaderFactory.CreateOpenXmlReader(excelStream);

                    excelReader.IsFirstRowAsColumnNames = hasHeaders;
                    DataSet xlData = excelReader.AsDataSet();
                    xlData.DataSetName = Path.GetFileNameWithoutExtension(filePath);
                    for (int i = xlData.Tables.Count - 1; i >= 0; i--)
                    {
                        if (!tableNames.Contains(xlData.Tables[i].TableName))
                            xlData.Tables.RemoveAt(i);
                    }

                    return xlData;

                }
                finally
                {
                    if (excelReader != null)
                    {
                        excelReader.Close();
                        excelReader.Dispose();
                    }
                }
            }
        }
#endif
        private static string _getExcelCreateTableCommand(DataTable table)
        {
            if (table == null)
                throw new ArgumentNullException("table");

            var sb = new StringBuilder("CREATE TABLE ");
            sb.AppendFormat("[{0}] (", table.TableName);

            foreach (DataColumn col in table.Columns)
            {
                sb.AppendFormat("[{0}] {1},", col.ColumnName, _getOleDbType(col.DataType).ToString());
            }


            sb = sb.Remove(sb.Length - 1, 1); // Remove the last comma
            sb.Append(")");

            return sb.ToString();
        }
        private static OleDbType _getOleDbType(Type inputType)
        {
            switch (inputType.FullName)
            {
                case "System.Boolean":
                    return OleDbType.Boolean;
                case "System.Int32":
                    return OleDbType.Integer;
                case "System.Single":
                    return OleDbType.Single;
                case "System.Double":
                    return OleDbType.Double;
                case "System.Decimal":
                    return OleDbType.Decimal;
                case "System.String":
                    return OleDbType.Char;
                case "System.Char":
                    return OleDbType.Char;
                case "System.Byte[]":
                    return OleDbType.Binary;
                default:
                    return OleDbType.Variant;
            }
        }

        #endregion

        #region Static Methods

        private static bool _columnEqual(object A, object B)
        {
            if (A == DBNull.Value && B == DBNull.Value)
                return true;

            if (A == DBNull.Value || B == DBNull.Value)
                return false;

            return (A.Equals(B));
        }
        private static string[] _getSchemaTables(DbConnection connection)
        {
            DataTable schema = _getSchemaCollection(connection, "Tables");
            return schema.AsEnumerable().Select(r => r.Field<string>("TABLE_NAME").Trim('\'')).ToArray();
        }
        private static DataTable _getSchemaCollection(DbConnection connection, string collectionName)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            return connection.GetSchema(collectionName);
        }

        public static List<string> SelectDistinct(DataTable source, string fieldName, string Filter = "")
        {
            var retList = new List<string>();
            object LastValue = null;

            foreach (DataRow dr in source.Select(Filter, fieldName))
            {
                if (LastValue == null || !(_columnEqual(LastValue, dr[fieldName])))
                {
                    LastValue = dr[fieldName];
                    retList.Add(LastValue.ToString());
                }
            }

            retList.Sort();
            if (!retList.Contains(string.Empty))
                retList.Insert(0, string.Empty);

            return retList;
        }
        public static DataSet FillDataSet(DbProviderFactory factory, DbConnection connection, params string[] tableNames)
        {
            if (factory == null)
                throw new ArgumentNullException("factory");

            if (connection == null)
                throw new ArgumentNullException("connection");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            if (tableNames == null || tableNames.Length == 0)
                tableNames = _getSchemaTables(connection);

            var returnSet = new DataSet();
            if (factory is OleDbFactory)  // OleDb provider doesn't allow multiple commands
            {
                foreach (string tableName in tableNames)
                {
                    using (DbCommand command = factory.CreateCommand())
                    using (DbDataAdapter adapter = factory.CreateDataAdapter())
                    {
                        string name = tableName;

                        command.Connection = connection;
                        if (!tableName.StartsWith("["))
                            name = "[" + name;
                        if (!tableName.EndsWith("]"))
                            name += "]";

                        command.CommandText = "SELECT * FROM " + name;

                        adapter.SelectCommand = command;

                        DataTable table = returnSet.Tables.Add();
                        adapter.Fill(table);
                    }
                }
            }
            else
            {
                using (DbCommand command = factory.CreateCommand())
                using (DbDataAdapter adapter = factory.CreateDataAdapter())
                {
                    var sb = new StringBuilder();
                    foreach (string tableName in tableNames)
                        sb.AppendFormat("SELECT * FROM {0};", tableName);

                    command.Connection = connection;
                    command.CommandText = sb.ToString();

                    adapter.SelectCommand = command;
                    adapter.Fill(returnSet);
                }
            }

            // Set the table names
            for (int i = 0; i < tableNames.Length; i++)
                returnSet.Tables[i].TableName = tableNames[i];

            return returnSet;
        }
        public static DataSet FillDataSet(DbProviderFactory factory, DbCommand command)
        {
            if (factory == null)
                throw new ArgumentNullException("factory");

            if (command == null)
                throw new ArgumentNullException("command");

            if (command.Connection == null)
                throw new InvalidOperationException("Command has no connection");

            if (command.Connection.State != ConnectionState.Open)
                command.Connection.Open();

            using (DbDataAdapter adapter = factory.CreateDataAdapter())
            {
                var returnSet = new DataSet();

                adapter.SelectCommand = command;
                adapter.Fill(returnSet);

                return returnSet;
            }
        }
        public static DataTable FillDataTable(DbProviderFactory factory, DbConnection connection, string tableName)
        {
            return FillDataSet(factory, connection, tableName).Tables[tableName];
        }
        public static DataTable FillDataTable(DbProviderFactory factory, DbCommand command, string tableName = "")
        {
            if (factory == null)
                throw new ArgumentNullException("factory");

            if (command == null)
                throw new ArgumentNullException("command");

            if (command.Connection == null)
                throw new InvalidOperationException("Command has no connection");

            if (command.Connection.State != ConnectionState.Open)
                command.Connection.Open();

            using (DbDataAdapter adapter = factory.CreateDataAdapter())
            {
                var returnSet = new DataSet();

                adapter.SelectCommand = command;
                adapter.Fill(returnSet);

                // Default to first table if one has not been provided
                // if tablename is not found, null is returned
                return string.IsNullOrEmpty(tableName)
                    ? returnSet.Tables[0]
                    : returnSet.Tables.Contains(tableName)
                        ? returnSet.Tables[tableName]
                        : null;
            }
        }
        public static DbCommand GetCommand(DbProviderFactory factory, DbTransaction transaction, string tableName, CommandDirective directive)
        {
            DbCommand command = GetCommand(factory, transaction.Connection, tableName, directive, transaction);
            command.Transaction = transaction;
            command.Connection = transaction.Connection;

            return command;
        }
        public static DbCommand GetCommand(DbProviderFactory factory, DbConnection connection, string tableName, CommandDirective directive, DbTransaction transaction = null)
        {
            if (factory == null)
                throw new ArgumentNullException("factory");

            if (connection == null)
                throw new ArgumentNullException("connection");

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException("tableName");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            DbCommandBuilder builder = factory.CreateCommandBuilder();
            if (builder is OleDbCommandBuilder)
            {
                if (!tableName.StartsWith("["))
                    tableName = "[" + tableName;
                if (!tableName.EndsWith("]"))
                    tableName += "]";

                builder.QuotePrefix = "[";
                builder.QuoteSuffix = "]";
            }

            DbCommand command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = "SELECT * FROM " + tableName;

            DbDataAdapter adapter = factory.CreateDataAdapter();
            builder.DataAdapter = adapter;

            var schemaTable = new DataTable();
            adapter.SelectCommand = command;
            adapter.FillSchema(schemaTable, SchemaType.Source);

            DbCommand returnCommand = null;
            switch (directive)
            {
                case CommandDirective.Update:
                    returnCommand = builder.GetUpdateCommand(true);
                    break;
                case CommandDirective.Insert:
                    returnCommand = builder.GetInsertCommand(true);
                    break;
                case CommandDirective.Delete:
                    returnCommand = builder.GetDeleteCommand(true);
                    break;
                default:
                    return null;
            }

            adapter.Dispose();

            return returnCommand;
        }
        public static int UpdateData(DbProviderFactory factory, DbConnection connection, DataTable table)
        {
            if (factory == null)
                throw new ArgumentNullException("factory");

            if (connection == null)
                throw new ArgumentNullException("connection");

            if (table == null)
                throw new ArgumentNullException("table");

            if (string.IsNullOrEmpty(table.TableName))
                throw new InvalidOperationException("DataTable must have a valid TableName");

            if (connection.State != ConnectionState.Open)
                connection.Open();

            using (DbTransaction transaction = connection.BeginTransaction())
            using (DbDataAdapter adapter = factory.CreateDataAdapter())
            {
                try
                {
                    // Check if there are any modifications
                    DataTable modTable = table.GetChanges();
                    if (modTable == null)
                        return 0;

                    bool isDeleting = modTable.AsEnumerable().Any(r => r.RowState == DataRowState.Deleted);
                    bool isInserting = modTable.AsEnumerable().Any(r => r.RowState == DataRowState.Added);
                    bool isUpdating = modTable.AsEnumerable().Any(r => r.RowState == DataRowState.Modified);

                    // Set the commands
                    if (isDeleting)
                        adapter.DeleteCommand = GetCommand(factory, transaction, table.TableName, CommandDirective.Delete);

                    if (isInserting)
                        adapter.InsertCommand = GetCommand(factory, transaction, table.TableName, CommandDirective.Insert);

                    if (isUpdating)
                        adapter.UpdateCommand = GetCommand(factory, transaction, table.TableName, CommandDirective.Update);

                    // Update the database
                    int result = adapter.Update(modTable);

                    transaction.Commit();

                    return result;
                }
                catch (DBConcurrencyException)
                {
                    // What do we do here?
                    throw new NotImplementedException();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        public static int UpdateData(DbProviderFactory factory, DbConnection connection, DataSet set)
        {
            if (factory == null)
                throw new ArgumentNullException("factory");

            if (connection == null)
                throw new ArgumentNullException("connection");

            if (set == null)
                throw new ArgumentNullException("set");

            int result = 0;

            foreach (DataTable table in set.Tables)
                result += UpdateData(factory, connection, table);

            return result;
        }

        #endregion

        #endregion
    }
}