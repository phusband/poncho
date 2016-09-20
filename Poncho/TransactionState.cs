namespace Poncho
{
    public enum TransactionState
    {
        None = 0,
        Open = 1 << 0,
        Committed = 1 << 1,
        RolledBack = 1 << 2,
        Disposed = 1 << 3
    }
}
