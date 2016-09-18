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
        Open = 2 << 0,
        Committed = 2 << 1,
        Completed = 2 << 2,
        RolledBack = 2 << 3
    }
}
