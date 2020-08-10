
using System;
using System.Collections.Generic;
using Transactions.Infrastructure.Network;

namespace Transactions.Scenarios.Common.Messages
{
    public static class AppMessages
    {
        public class TransferTx : ICloneable
        {
            public string Donor { get; private set; }
            public string Recipient { get; private set; }
            public int Amount { get; private set; }

            public TransferTx(string donor, string recipient, int amount)
            {
                this.Donor = donor;
                this.Recipient = recipient;
                this.Amount = amount;
            }

            public object Clone()
            {
                return new TransferTx(this.Donor, this.Recipient, this.Amount);
            }
        }

        public class ReadTx : ICloneable
        {
            public List<string> Accounts { get; } = new List<string>();

            public object Clone()
            {
                var cloned = new ReadTx();
                cloned.Accounts.AddRange(this.Accounts);
                return cloned;
            }
        }

        public abstract class TxResult<T> : ICloneable
            where T : TxResult<T>, new()
        {
            protected readonly Dictionary<string, int> accounts = new Dictionary<string, int>();

            public Dictionary<string, int> Accounts => this.accounts;

            public object Clone()
            {
                var clone = new T();

                foreach (var account in this.Accounts.Keys)
                {
                    clone.Accounts[account] = this.Accounts[account];
                }

                return clone;
            }
        }
        
        public class TxOk : TxResult<TxOk> {}

        public class TxInsufficientFunds : TxResult<TxInsufficientFunds> {}
    }
}