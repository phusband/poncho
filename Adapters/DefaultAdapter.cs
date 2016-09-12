using System.Data.Common;
namespace Poncho.Adapters
{
    internal sealed class DefaultAdapter : DbAdapter
    {
        internal DefaultAdapter(DbProviderFactory factory, DbConnection baseConnection, string connectionString)
            : base(factory, baseConnection, connectionString) { }
    }
}
