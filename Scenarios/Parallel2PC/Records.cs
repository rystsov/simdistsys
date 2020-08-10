using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.Parallel2PC
{
    public static class Records
    {
        public class Account3 : ICloneable
        {
            public string LockedByTx { get; set; }
            public List<string> TxKeys { get; set; }
            public int CurrValue { get; set; }
            public int NextValue { get; set; }
            public ulong Version { get; set; }

            public object Clone()
            {
                return new Account3
                { 
                    Version = this.Version,
                    CurrValue = this.CurrValue,
                    NextValue = this.NextValue,
                    LockedByTx = this.LockedByTx,
                    TxKeys = this.TxKeys == null ? null : new List<string>(this.TxKeys)
                };
            }
        }

        public class Tx : ICloneable
        {
            public object Clone()
            {
                return this;
            }
        }
    }
}