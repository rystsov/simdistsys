using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Transactions.Scenarios.RW2PC
{
    public static class Records
    {
        public class Account : ICloneable
        {
            public string LockedByTx { get; set; }
            public HashSet<string> ReadLocks { get; set; }
            public List<string> TxKeys { get; set; }
            public int Value { get; set; }

            public object Clone()
            {
                return new Account
                { 
                    Value = this.Value,
                    LockedByTx = this.LockedByTx,
                    ReadLocks = this.ReadLocks == null ? null : new HashSet<string>(this.ReadLocks),
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