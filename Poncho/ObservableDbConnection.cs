using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Runtime.Remoting;
using System.Threading;
using System.Threading.Tasks;
using Poncho.Adapters;

namespace Poncho
{
    /// <summary>
    /// Represents a connection to a database that can be observed for <see cref="T:System.Data.ConnectionState" /> changes or <see cref="T:Poncho.ObservableDbConnection" /> operations.
    /// Can create new <see cref="T:Poncho.ObservableDbCommand" /> objects.
    /// </summary>
    [System.ComponentModel.DesignerCategory("Code")]
    public sealed class ObservableDbConnection : DbConnection
    {
        #region Properties

        private static readonly object _commandLock = new object();
        private static readonly object _transactionLock = new object();

        private readonly IList<ObservableDbCommand> _activeCommands = new List<ObservableDbCommand>();
        private readonly IList<ObservableDbTransaction> _activeTransactions = new List<ObservableDbTransaction>();
        private readonly DbAdapter _dbAdapter;
        private bool _disposed;
        private readonly DbConnection _baseConnection;

        protected override bool CanRaiseEvents
        {
            get
            {
                return true;
            }
        }
        protected override DbProviderFactory DbProviderFactory
        {
            get
            {
                return DbAdapter?.ProviderFactory;
            }
        }

        public DbAdapter DbAdapter => _dbAdapter;
        public ICollection<ObservableDbCommand> ActiveCommands
        {
            get
            {
                lock (_commandLock)
                {
                    return _activeCommands;
                }
            }
        }
        public ICollection<ObservableDbTransaction> ActiveTransactions
        {
            get
            {
                lock(_transactionLock)
                {
                    return _activeTransactions;
                }
            }
        }

        /// <summary>Gets the base underlying base <see cref="T:System.Data.Common.DbConnection" />. </summary>
        public DbConnection BaseConnection
        {
            get { return _baseConnection; }
        }

        /// <summary>Gets or sets the string used to open the connection.</summary>
        public override string ConnectionString
        {
            get { return _baseConnection?.ConnectionString; }
            set
            {
                if (_baseConnection != null)
                    _baseConnection.ConnectionString = value;
            }
        }

        /// <summary>Gets the time to wait while establishing a connection before terminating the attemt and throwing an error.</summary>
        public override int ConnectionTimeout
        {
            get
            {
                return _baseConnection.ConnectionTimeout;
            }
        }

        /// <summary>Gets the name of the current database after a connection is opened, or the database name specified in the connection string before the connection is opened.</summary>
        public override string Database
        {
            get { return _baseConnection?.Database; }
        }

        /// <summary>Gets the name of the database server to which to connect.</summary>
        public override string DataSource
        {
            get { return _baseConnection?.DataSource; }
        }

        /// <summary>Gets a string that represents the version of the server to which the object is connected to. </summary>
        public override string ServerVersion
        {
            get { return _baseConnection?.ServerVersion; }
        }

        /// <summary>Gets a string that describes the current state of the connection.</summary>
        public override ConnectionState State
        {
            get { return _baseConnection?.State ?? ConnectionState.Closed; }
        }

        #endregion

        #region Events

        public event EventHandler<StateChangeEventArgs> Broken;
        public event EventHandler<StateChangeEventArgs> Closed;
        public event EventHandler<StateChangeEventArgs> Connecting;
        public event EventHandler<StateChangeEventArgs> Executing;
        public event EventHandler<StateChangeEventArgs> Fetching;
        public event EventHandler<StateChangeEventArgs> Opened;
        public override event StateChangeEventHandler StateChange;

        public delegate void BrokenEventHandler(object sender, StateChangeEventArgs e);
        public delegate void ClosedEventHandler(object sender, StateChangeEventArgs e);
        public delegate void ConnectingEventHandler(object sender, StateChangeEventArgs e);
        public delegate void ExecutingEventHandler(object sender, StateChangeEventArgs e);
        public delegate void FetchingEventHandler(object sender, StateChangeEventArgs e);
        public delegate void OpenedEventHandler(object sender, StateChangeEventArgs e);

        #endregion

        #region Constructors

        internal ObservableDbConnection(DbAdapter dbAdapter, DbConnection baseConnection)
        {
            if (dbAdapter == null)
                throw new ArgumentNullException("dbAdapter");

            if (baseConnection == null)
                throw new ArgumentNullException("baseConnection");

            _baseConnection = baseConnection;
            _dbAdapter = dbAdapter;

            _baseConnection.StateChange += (o, e) => { OnStateChange(e); };  // Subscribes
        }

        #endregion

        #region Methods

        private void checkBaseConnection()
        {
            if (_baseConnection == null)
                throw new InvalidOperationException("Base connection is not available.");
        }
        private void commandDisposal(object o, EventArgs e)
        {
            var cmd = o as ObservableDbCommand;
            if (cmd != null)
            {
                lock (_commandLock)
                {
                    _activeCommands?.Remove(cmd);
                }
            }
        }

        /// <summary>Starts a database transaction.</summary>
        /// <param name="isolation"></param>
        /// <returns></returns>
        public new ObservableDbTransaction BeginTransaction(IsolationLevel isolation = IsolationLevel.ReadCommitted)
        {
            var transaction = (ObservableDbTransaction)BeginDbTransaction(isolation);
            lock (_transactionLock)
            {
                _activeTransactions.Add(transaction);
            }

            return transaction;
        }

        /// <summary>Changes the current database for an open connection.</summary>
        /// <param name="databaseName">Specifies the name of the database for the connection to use.</param>
        public override void ChangeDatabase(string databaseName)
        {
            checkBaseConnection();
            _baseConnection.ChangeDatabase(databaseName);
        }

        /// <summary>Closes the connection to the database.  This is the preffered method of closing any open connection.</summary>
        public override void Close()
        {
            checkBaseConnection();
            _baseConnection.Close();
        }

        /// <summary>Creates and returns a <see cref="T:Poncho.ObservableDbCommand" /> object associated with the current connection.</summary>
        /// <returns>A <see cref="T:Poncho.ObservableDbCommand" /> object.</returns>
        public new ObservableDbCommand CreateCommand()
        {
            var command = (ObservableDbCommand)CreateDbCommand();
            lock (_commandLock)
            {
                _activeCommands.Add(command);
            }

            command.Disposed += commandDisposal;
            return command;
        }

        /// <summary>Creates and returns a <see cref="T:Poncho.ObservableDbCommand" /> object associated with the current connection.</summary>
        /// <returns>A <see cref="T:Poncho.ObservableDbCommand" /> object.</returns>
        /// <param name="transaction">Specifies the <see cref="T:System.Data.Common.DbTransaction" /> for the command to use.</param>
        public ObservableDbCommand CreateCommand(DbTransaction transaction)
        {
            var command = CreateCommand();
            command.Transaction = transaction;

            return command;
        }

        /// <summary>Creates and returns a <see cref="T:Poncho.ObservableDbCommand" /> object associated with the current connection.</summary>
        /// <returns>A <see cref="T:Poncho.ObservableDbCommand" /> object.</returns>
        /// <param name="commandText">Specifies the CommandText string for the command to use.</param>
        /// <param name="commandType">Specifies the <see cref="T:System.Data.CommandType" /> for the command to use.</param>
        public ObservableDbCommand CreateCommand(string commandText, CommandType commandType = CommandType.Text)
        {
            var command = CreateCommand();
            command.CommandText = commandText;
            command.CommandType = commandType;

            return command;
        }

        /// <summary>Creates and returns a <see cref="T:Poncho.ObservableDbCommand" /> object associated with the current connection.</summary>
        /// <returns>A <see cref="T:Poncho.ObservableDbCommand" /> object.</returns>
        /// <param name="transaction">Specifies the <see cref="T:System.Data.Common.DbTransaction" /> for the command to use.</param>
        /// <param name="commandText">Specifies the CommandText string for the command to use.</param>
        /// <param name="commandType">Specifies the <see cref="T:System.Data.CommandType" /> for the command to use.</param>
        public ObservableDbCommand CreateCommand(DbTransaction transaction, string commandText, CommandType commandType = CommandType.Text)
        {
            var command = CreateCommand(commandText, commandType);
            command.Transaction = transaction;

            return command;
        }

        /// <summary>Enlists in the specified transaction.</summary>
        /// <param name="transaction">A reference to an existing System.Transactions.Transaction in which to enlist.</param>
        public override void EnlistTransaction(System.Transactions.Transaction transaction)
        {
            _baseConnection.EnlistTransaction(transaction);
        }

        /// <summary>Returns schema information for the data source of this <see cref="T:Poncho.ObservableDbConnection" />.</summary>
        public override DataTable GetSchema()
        {
            return _baseConnection.GetSchema();
        }

        /// <summary>Returns schema information for the data source of this <see cref="T:Poncho.ObservableDbConnection" />.</summary>
        public override DataTable GetSchema(string collectionName)
        {
            return _baseConnection.GetSchema(collectionName);
        }

        /// <summary>Returns schema information for the data source of this <see cref="T:Poncho.ObservableDbConnection" />.</summary>
        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            return _baseConnection.GetSchema(collectionName, restrictionValues);
        }

        /// <summary>Opens a database connection with the settings specified by the <see cref="T:Poncho.ObservableDbConnection" />.ConnectionString.</summary>
        public override void Open()
        {
            checkBaseConnection();
            _baseConnection.Open();
        }

        public new Task OpenAsync()
        {
            return _baseConnection.OpenAsync();
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            return _baseConnection.OpenAsync(cancellationToken);
        }

        public override ObjRef CreateObjRef(Type requestedType)
        {
            return _baseConnection.CreateObjRef(requestedType);
        }

        public override ISite Site
        {
            get
            {
                return _baseConnection.Site;
            }

            set
            {
                if (_baseConnection != null)
                    _baseConnection.Site = value;
            }
        }

        public override object InitializeLifetimeService()
        {
            return _baseConnection.InitializeLifetimeService();
        }

        protected override object GetService(Type service)
        {
            return base.GetService(service);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            var transaction = new ObservableDbTransaction(this, isolationLevel);

            return transaction;
        }
        protected override DbCommand CreateDbCommand()
        {
            if (DbAdapter == null)
                throw new InvalidOperationException("DBAdapter is not available.");

            var command = DbAdapter.CreateCommand();
            command.Connection = this;
            command.CommandTimeout = _dbAdapter.Timeout ?? command.CommandTimeout;

            return command;
        }
        protected override void OnStateChange(StateChangeEventArgs stateChange)
        {
            var stateChangeCopy = StateChange;
            stateChangeCopy?.Invoke(this, stateChange);

            switch (stateChange.CurrentState)
            {
                case ConnectionState.Broken:
                    var brokenCopy = Broken;
                    brokenCopy?.Invoke(this, stateChange);
                    break;
                case ConnectionState.Closed:
                    var closedCopy = Closed;
                    closedCopy?.Invoke(this, stateChange);
                    break;
                case ConnectionState.Connecting:
                    var connectingCopy = Connecting;
                    connectingCopy?.Invoke(this, stateChange);
                    break;
                case ConnectionState.Executing:
                    var executingCopy = Executing;
                    executingCopy?.Invoke(this, stateChange);
                    break;
                case ConnectionState.Fetching:
                    var fetchingCopy = Fetching;
                    fetchingCopy?.Invoke(this, stateChange);
                    break;
                case ConnectionState.Open:
                    var openedCopy = Opened;
                    openedCopy?.Invoke(this, stateChange);
                    break;
            }
        }

        #endregion

        #region IDisposable

        public new void Dispose()
        {
            this.Close();
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    lock (_commandLock)
                    {
                        foreach (var command in _activeCommands)
                        {
                            command?.Cancel();
                            command?.Dispose();
                        }
                    }

                    lock (_transactionLock)
                    {
                        foreach (var transaction in _activeTransactions)
                        {
                            transaction?.Rollback();
                            transaction?.Dispose();
                        }
                    }
                }

                _disposed = true;
            }

            base.Dispose(disposing);
            _baseConnection.Dispose();
        }

        #endregion
    }
}
