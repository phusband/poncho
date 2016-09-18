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
        private bool _completed;
        private readonly DbAdapter _dbAdapter;
        private bool _disposed;

        protected override DbConnection DbConnection => _baseTransaction?.Connection;

        /// <summary>Gets the base underlying base <see cref="System.Data.Common.DbTransaction" />. </summary>
        public DbTransaction BaseTransaction => _baseTransaction;

        /// <summary>Gets the <see cref="Poncho.Adapters.DbAdapter" /> the transaction was created from.</summary>
        public DbAdapter DbAdapter => _dbAdapter;

        /// <summary>Specifies the <see cref="System.Data.IsolationLevel"/> for this transaction.</summary>
        public override IsolationLevel IsolationLevel
        {
            get { return _baseTransaction.IsolationLevel; }
        }

        #endregion

        #region Events

        /// <summary>Occurs when the transaction is committed.</summary>
        public event EventHandler<ObservableDbTransactionEventArgs> Committed;

        /// <summary>Occurs when the transaction ic completed.</summary>
        public event EventHandler<ObservableDbTransactionEventArgs> Completed;

        /// <summary>Occurs when the transaction is disposed.</summary>
        public event EventHandler<ObservableDbTransactionEventArgs> Disposed;

        /// <summary>Occurs when the transaction is rolled back.</summary>
        public event EventHandler<ObservableDbTransactionEventArgs> RolledBack;

        #endregion

        #region Constructors

        internal ObservableDbTransaction(ObservableDbConnection connection, IsolationLevel isolation = IsolationLevel.ReadCommitted)
            : this(connection.DbAdapter, connection.BeginTransaction(isolation)) { }
        internal ObservableDbTransaction(DbAdapter adapter, DbTransaction baseTransaction)
        {
            _dbAdapter = adapter;
            _baseTransaction = baseTransaction;
            this.Completed += (o, e) => { _completed = true; };
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
            committedCopy?.Invoke(this, new ObservableDbTransactionEventArgs(this, TransactionState.Committed));

            var completedCopy = Completed;
            completedCopy?.Invoke(this, new ObservableDbTransactionEventArgs(this, TransactionState.Completed));
        }

        /// <summary>Rolls back a transaction from a pending state.</summary>
        public override void Rollback()
        {
            checkBaseTransaction();

            _baseTransaction.Rollback();

            var rolledBackCopy = RolledBack;
            rolledBackCopy?.Invoke(this, new ObservableDbTransactionEventArgs(this, TransactionState.RolledBack));

            var completedCopy = Completed;
            completedCopy?.Invoke(this, new ObservableDbTransactionEventArgs(this, TransactionState.Completed));
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
                    disposedCopy?.Invoke(this, new ObservableDbTransactionEventArgs(this, TransactionState.Disposed));
                    _disposed = true;
                }
            }
            base.Dispose(disposing);
            BaseTransaction.Dispose();
        }

        #endregion
    }
}
