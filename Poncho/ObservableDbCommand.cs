using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Poncho.Adapters;

namespace Poncho
{
    [System.ComponentModel.DesignerCategory("Code")]
    public sealed class ObservableDbCommand : DbCommand
    {
        #region Properties

        private readonly DbCommand _baseCommand;
        private readonly ObservableDbConnection _connection;
        private bool _disposed;
        private ObservableDbTransaction _transaction;

        protected override bool CanRaiseEvents
        {
            get { return base.CanRaiseEvents; }
        }
        protected override DbConnection DbConnection
        {
            get { return _baseCommand.Connection; }
            set
            {
                if (value == null)
                    return;

                _baseCommand.Connection = value;
                _connection.BaseConnection = value;
            }
        }
        protected override DbParameterCollection DbParameterCollection => _baseCommand.Parameters;
        protected override DbTransaction DbTransaction
        {
            get { return _baseCommand.Transaction; }
            set
            {
                _baseCommand.Transaction = value;
                _transaction = new ObservableDbTransaction(Connection, value);
            }
        }

        public DbCommand BaseCommand => _baseCommand;
        public override string CommandText
        {
            get { return _baseCommand.CommandText; }
            set { _baseCommand.CommandText = value; }
        }
        public override int CommandTimeout
        {
            get { return _baseCommand.CommandTimeout; }
            set { _baseCommand.CommandTimeout = value; }
        }
        public override CommandType CommandType
        {
            get { return _baseCommand.CommandType; }
            set { _baseCommand.CommandType = value; }
        }
        public new ObservableDbConnection Connection => _connection;
        public DbAdapter DbAdapter => _connection.DbAdapter;
        [Browsable(false), DefaultValue(true), DesignOnly(true), EditorBrowsable(EditorBrowsableState.Never)]
        public override bool DesignTimeVisible
        {
            get { return _baseCommand.DesignTimeVisible; }
            set { _baseCommand.DesignTimeVisible = value; }
        }
        public new ObservableDbTransaction Transaction
        {
            get { return _transaction; }
            set
            {
                _transaction = value;
                _baseCommand.Transaction = _transaction.BaseTransaction;
            }
        }
        public override UpdateRowSource UpdatedRowSource
        {
            get { return _baseCommand.UpdatedRowSource; }
            set { _baseCommand.UpdatedRowSource = value; }
        }

        #endregion

        #region Events

        /// <summary>Occurs when the command executes an operation.</summary>
        public event EventHandler<CommandOperationEventArgs> OperationExecuted;

        #endregion

        #region Constructors

        internal ObservableDbCommand(ObservableDbConnection connection, DbCommand baseCommand)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (baseCommand == null)
                throw new ArgumentNullException("baseCommand");

            _connection = connection;
            _baseCommand = baseCommand;
            _baseCommand.Connection = connection.BaseConnection;
        }

        #endregion

        #region Methods

        /// <summary>Attempts to cancel the execution of a <see cref="Poncho.ObservableDbCommand"/>.</summary>
        public override void Cancel()
        {
            checkBaseCommand();
            _baseCommand.Cancel();

            var operationCopy = OperationExecuted;
            operationCopy?.Invoke(this, new CommandOperationEventArgs(this, null, CommandOperation.Cancelled));
        }

        public new DbDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
        {
            var result = ExecuteDbDataReader(behavior);

            var operationCopy = OperationExecuted;
            operationCopy?.Invoke(this, new CommandOperationEventArgs(this, result, CommandOperation.ExecuteReader));

            return result;
        }

        public new Task ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var result = ExecuteDbDataReaderAsync(behavior, cancellationToken);

            var operationCopy = OperationExecuted;
            operationCopy?.Invoke(this, new CommandOperationEventArgs(this, result, CommandOperation.ExecuteReader | CommandOperation.Async));

            return result;
        }

        /// <summary>Executes an SQL statement against a connection object.</summary>
        public override int ExecuteNonQuery()
        {
            checkBaseCommand();
            var result = _baseCommand.ExecuteNonQuery();

            var operationCopy = OperationExecuted;
            operationCopy?.Invoke(this, new CommandOperationEventArgs(this, result, CommandOperation.ExecuteNonQuery));

            return result;
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            checkBaseCommand();
            var result = _baseCommand.ExecuteNonQueryAsync(cancellationToken);

            var operationCopy = OperationExecuted;
            operationCopy?.Invoke(this, new CommandOperationEventArgs(this, result, CommandOperation.ExecuteNonQuery | CommandOperation.Async));

            return result;
        }

        public override object ExecuteScalar()
        {
            checkBaseCommand();
            var result = _baseCommand.ExecuteScalar();

            var operationCopy = OperationExecuted;
            operationCopy?.Invoke(this, new CommandOperationEventArgs(this, result, CommandOperation.ExecuteScalar));

            return result;
        }

        public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            checkBaseCommand();
            var result = _baseCommand.ExecuteScalarAsync(cancellationToken);

            var operationCopy = OperationExecuted;
            operationCopy?.Invoke(this, new CommandOperationEventArgs(this, result, CommandOperation.ExecuteScalar | CommandOperation.Async));

            return result;
        }

        public override void Prepare()
        {
            checkBaseCommand();
            _baseCommand.Prepare();
        }

        protected override DbParameter CreateDbParameter()
        {
            checkBaseCommand();
            return _baseCommand.CreateParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            checkBaseCommand();
            return _baseCommand.ExecuteReader(behavior);
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            checkBaseCommand();
            return _baseCommand.ExecuteReaderAsync(behavior, cancellationToken);
        }

        private void checkBaseCommand()
        {
            if (_baseCommand == null)
                throw new InvalidOperationException("Base DbCommand is not available.");
        }

        #endregion

        #region IDisposable

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _baseCommand.Dispose();
                    _disposed = true;
                }
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
