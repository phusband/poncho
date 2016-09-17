using System;
using System.Data;
using System.Data.Common;
using Poncho.Adapters;

namespace Poncho
{
    [System.ComponentModel.DesignerCategory("Code")]
    public sealed class ObservableDbConnection : DbConnection
    {
        #region Properties

        private readonly DbAdapter _dbAdapter;
        private readonly DbConnection _baseConnection;

        public DbAdapter DbAdapter => _dbAdapter;

        #endregion

        #region Events

        public event EventHandler<StateChangeEventArgs> Broken;
        public event EventHandler<StateChangeEventArgs> Closed;
        public event EventHandler<StateChangeEventArgs> Connecting;
        public event EventHandler<StateChangeEventArgs> Executing;
        public event EventHandler<StateChangeEventArgs> Fetching;
        public event EventHandler<StateChangeEventArgs> Opened;

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

            _baseConnection.StateChange += stateChanged;  // Subscribes
        }

        #endregion

        #region Methods

        /// <summary>Creates and returns a <see cref="T:Poncho.ObservableDbCommand" /> object associated with the current connection.</summary>
		/// <returns>A <see cref="T:Poncho.ObservableDbCommand" /> object.</returns>
        public new ObservableDbCommand CreateCommand()
        {
            return (ObservableDbCommand)CreateDbCommand();
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

        private void checkBaseConnection()
        {
            if (_baseConnection == null)
                throw new InvalidOperationException("Base connection is not available.");
        }
        private void stateChanged(object sender, StateChangeEventArgs e)
        {
            switch (e.CurrentState)
            {
                case ConnectionState.Broken:
                    var brokenCopy = Broken;
                    brokenCopy?.Invoke(this, e);
                    break;
                case ConnectionState.Closed:
                    var closedCopy = Closed;
                    closedCopy?.Invoke(this, e);
                    break;
                case ConnectionState.Connecting:
                    var connectingCopy = Connecting;
                    connectingCopy?.Invoke(this, e);
                    break;
                case ConnectionState.Executing:
                    var executingCopy = Executing;
                    executingCopy?.Invoke(this, e);
                    break;
                case ConnectionState.Fetching:
                    var fetchingCopy = Fetching;
                    fetchingCopy?.Invoke(this, e);
                    break;
                case ConnectionState.Open:
                    var openedCopy = Opened;
                    openedCopy?.Invoke(this, e);
                    break;
            }
        }

        #endregion

        #region DbConnection

        public override string ConnectionString
        {
            get { return _baseConnection?.ConnectionString; }
            set
            {
                if (_baseConnection != null)
                    _baseConnection.ConnectionString = value;
            }
        }
        public override string Database
        {
            get { return _baseConnection?.Database; }
        }
        public override string DataSource
        {
            get { return _baseConnection?.DataSource; }
        }
        public override string ServerVersion
        {
            get { return _baseConnection?.ServerVersion; }
        }
        public override ConnectionState State
        {
            get { return _baseConnection?.State ?? ConnectionState.Closed; }
        }

        /// <summary>
        /// Changes the current database for an open connection.
        /// </summary>
        /// <param name="databaseName">Specifies the name of the database for the connection to use.</param>
        public override void ChangeDatabase(string databaseName)
        {
            checkBaseConnection();
            _baseConnection.ChangeDatabase(databaseName);
        }

        /// <summary>
        /// Closes the connection to the database.  This is the preffered method of closinh any open connection.
        /// </summary>
        public override void Close()
        {
            checkBaseConnection();
            _baseConnection.Close();
        }

        /// <summary>
        /// Opens a database connection with the settings specified by the <see cref="T:Poncho.ObservableDbConnection" />.ConnectionString.
        /// </summary>
        public override void Open()
        {
            checkBaseConnection();
            _baseConnection.Open();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return DbAdapter?.BeginTransaction(isolationLevel);
        }
        protected override DbCommand CreateDbCommand()
        {
            return DbAdapter.CreateCommand();
        }

        #endregion
    }
}
