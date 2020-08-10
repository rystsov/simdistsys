using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Transactions.Scenarios.OCC2PC
{
    public static class Records
    {
        public class Account : ICloneable
        {
            public string LockedByTx { get; set; }
            public List<string> TxKeys { get; set; }
            public int Value { get; set; }
            public ulong Version { get; set; }

            public object Clone()
            {
                return new Account
                { 
                    Value = this.Value,
                    LockedByTx = this.LockedByTx,
                    Version = this.Version,
                    TxKeys = this.TxKeys == null ? null : new List<string>(this.TxKeys)
                };
            }
        }

        public class Tx : ICloneable
        {
            public enum TxState { Committed, Aborted }

            public TxState State { get; set; }
            
            public Dictionary<string, int> Values { get; set; }

            public object Clone()
            {
                return new Tx
                { 
                    State = this.State,
                    Values = this.Values == null ? null : this.Values.ToDictionary(x => x.Key, x => x.Value)
                };
            }
        }
    }
}