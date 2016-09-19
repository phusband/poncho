namespace Poncho
{
    public enum CommandOperation
    {
        None = 0,
        Cancelled = 1 << 0,
        ExecuteNonQuery = 1 << 1,
        ExecuteScalar = 1 << 2,
        ExecuteReader = 1 << 3,
        Async = 1 << 4,
        Disposed = 1 << 5
    }
}
