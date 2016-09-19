using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Poncho
{
    public enum TransactionState
    {
        Disposed = 0,
        Open = 1 << 0,
        Committed = 1 << 1,
        Completed = 1 << 2,
        RolledBack = 1 << 3
    }
}
