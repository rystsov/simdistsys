using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;

namespace Transactions.Scenarios.Transactionless
{
    public class AccountRecord : ICloneable
    {
        public int Value { get; set; }

        public object Clone()
        {
            return new AccountRecord { Value = this.Value };
        }
    }
}