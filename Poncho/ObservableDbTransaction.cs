using System;
using System.Data;
using System.Data.Common;

namespace Poncho
{
    public sealed class ObservableDbTransaction : DbTransaction
    {
        #region Properties

        private readonly DbTransaction _baseTransaction;

        #endregion

        #region Events

        public event EventHandler<EventArgs> Committed;
        public event EventHandler<EventArgs> Completed;
        public event EventHandler<EventArgs> Disposed;
        public event EventHandler<EventArgs> RolledBack;

        public delegate void CommittedEventHandler(object sender, EventArgs e);
        public delegate void CompletedEventHandler(object sender, EventArgs e);
        public delegate void DisposedEventHandler(object sender, EventArgs e);
        public delegate void RolledBackEventHandler(object sender, EventArgs e);

        #endregion

        #region Constructors

        internal ObservableDbTransaction(DbConnection connection, IsolationLevel isolation = IsolationLevel.ReadCommitted)
            : this(connection.BeginTransaction(isolation)) { }
        internal ObservableDbTransaction(DbTransaction baseTransaction)
        {
            _baseTransaction = baseTransaction;
        }

        #endregion

        #region DbTransaction

        protected override DbConnection DbConnection
        {
            get { return _baseTransaction.Connection; }
        }
        public override IsolationLevel IsolationLevel
        {
            get { return _baseTransaction.IsolationLevel; }
        }

        /// <summary>
        /// Commits the database transaction.
        /// </summary>
        public override void Commit()
        {
            _baseTransaction?.Commit();
            var committedCopy = Committed;
            committedCopy?.Invoke(this, new EventArgs());

            var completedCopy = Completed;
            completedCopy?.Invoke(this, new EventArgs());
        }

        /// <summary>
        /// Rolls back a transaction from a pending state.
        /// </summary>
        public override void Rollback()
        {
            _baseTransaction?.Rollback();

            var rolledBackCopy = RolledBack;
            rolledBackCopy?.Invoke(this, new EventArgs());

            var completedCopy = Completed;
            completedCopy?.Invoke(this, new EventArgs());
        }

        #endregion

        #region IDisposable

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
                Disposed?.Invoke(this, new EventArgs());
        }

        #endregion
    }
}
