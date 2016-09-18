using System;

namespace Poncho
{
    public sealed class ObservableDbTransactionEventArgs : EventArgs
    {
        private readonly TransactionState _state;
        private readonly ObservableDbTransaction _transaction;

        public TransactionState State => _state;
        public ObservableDbTransaction Transaction => _transaction;

        public ObservableDbTransactionEventArgs(ObservableDbTransaction transaction, TransactionState state = TransactionState.Open)
        {
            _state = state;
            _transaction = transaction;
        }
    }
}
