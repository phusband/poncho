using System;
using System.Data;
using System.Data.Common;
using Poncho.Adapters;

namespace Poncho
{
    public sealed class ObservableDbTransaction : DbTransaction
    {
        #region Properties

        private readonly DbTransaction _baseTransaction;
        private readonly ObservableDbConnection _connection;
        private bool _disposed;

        protected override DbConnection DbConnection => _baseTransaction?.Connection;

        /// <summary>Gets the base underlying base <see cref="System.Data.Common.DbTransaction" />. </summary>
        public DbTransaction BaseTransaction => _baseTransaction;

        /// <summary>Specifies the <see cref="Poncho.ObservableDbConnection"/> object associated with the transaction.</summary>
        public new ObservableDbConnection Connection => _connection;

        /// <summary>Gets the <see cref="Poncho.Adapters.DbAdapter" /> the transaction was created from.</summary>
        public DbAdapter DbAdapter => Connection?.DbAdapter;

        /// <summary>Specifies the <see cref="System.Data.IsolationLevel"/> for this transaction.</summary>
        public override IsolationLevel IsolationLevel
        {
            get { return _baseTransaction?.IsolationLevel ?? IsolationLevel.Unspecified; }
        }

        #endregion

        #region Events

        /// <summary>Occurs when the transaction is committed.</summary>
        public event EventHandler<TransactionStateChangeEventArgs> Committed;

        /// <summary>Occurs when the transaction ic completed.</summary>
        public event EventHandler<TransactionStateChangeEventArgs> Completed;

        /// <summary>Occurs when the transaction is disposed.</summary>
        public event EventHandler<TransactionStateChangeEventArgs> Disposed;

        /// <summary>Occurs when the transaction is rolled back.</summary>
        public event EventHandler<TransactionStateChangeEventArgs> RolledBack;

        #endregion

        #region Constructors

        internal ObservableDbTransaction(ObservableDbConnection connection, IsolationLevel isolation = IsolationLevel.ReadCommitted)
            : this(connection, connection.BaseConnection?.BeginTransaction(isolation)) { }
        internal ObservableDbTransaction(ObservableDbConnection connection, DbTransaction baseTransaction)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (baseTransaction == null)
                throw new ArgumentNullException("baseTransaction");

            _baseTransaction = baseTransaction;
        }

        #endregion

        #region Methods

        private void checkBaseTransaction()
        {
            if (_baseTransaction == null)
                throw new InvalidOperationException("Base transaction is not available.");
        }

        /// <summary>Commits the database transaction.</summary>
        public override void Commit()
        {
            checkBaseTransaction();

            _baseTransaction.Commit();

            var committedCopy = Committed;
            committedCopy?.Invoke(this, new TransactionStateChangeEventArgs(this, TransactionState.Committed));

            var completedCopy = Completed;
            completedCopy?.Invoke(this, new TransactionStateChangeEventArgs(this, TransactionState.Completed));
        }

        /// <summary>Rolls back a transaction from a pending state.</summary>
        public override void Rollback()
        {
            checkBaseTransaction();

            _baseTransaction.Rollback();

            var rolledBackCopy = RolledBack;
            rolledBackCopy?.Invoke(this, new TransactionStateChangeEventArgs(this, TransactionState.RolledBack));

            var completedCopy = Completed;
            completedCopy?.Invoke(this, new TransactionStateChangeEventArgs(this, TransactionState.Completed));
        }

        #endregion

        #region IDisposable

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    var disposedCopy = Disposed;
                    disposedCopy?.Invoke(this, new TransactionStateChangeEventArgs(this, TransactionState.Disposed));
                    _disposed = true;
                }
            }
            base.Dispose(disposing);
            BaseTransaction.Dispose();
        }

        #endregion
    }
}
