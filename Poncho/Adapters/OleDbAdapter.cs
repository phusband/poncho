using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Poncho.Adapters
{
    internal class OleDbAdapter : DbAdapter
    {
        public OleDbAdapter(DbProviderFactory factory, DbConnection baseConnection, string connectionString)
            : base(factory, baseConnection, connectionString)
        {
            string properties = (ConnectionStringBuilder["Extended Properties"] as string ?? string.Empty).ToLowerInvariant();
            string oledbProvider = (ConnectionStringBuilder["Provider"] as string ?? string.Empty).ToLowerInvariant();
            string sourceFile = (ConnectionStringBuilder["Data Source"] as string ?? string.Empty).ToLowerInvariant();

            bool useQuoteBrackets = properties.Contains("excel") ||
                                    oledbProvider.Contains("ms remote") ||
                                    sourceFile.EndsWith(".accdb") ||
                                    sourceFile.EndsWith(".mdb");

            if (useQuoteBrackets)
            {
                CommandBuilder.QuotePrefix = "[";
                CommandBuilder.QuoteSuffix = "]";
            }
        }
    }
}
