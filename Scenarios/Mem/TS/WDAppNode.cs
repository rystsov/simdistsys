using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;
using Transactions.Scenarios.Common.Messages;
using Transactions.Scenarios.Parallel2PC;

namespace Transactions.Scenarios.Mem.TS
{
    public class WDAppNode : TSComparerAppNode
    {
        class InversedComparer : IComparer<long>
        {
            public int Compare(long a, long b)
            {
                return b.CompareTo(a);
            }
        }
        
        public WDAppNode(MaterializedLocksTMFactory createTM, IEndpoint network, IClock clock, IRandom random, string address, Func<string, string> shardLocator, Func<string, string> appLocator, long backoffCapUs, int attemptsPerIncrease, bool shouldReuseTime) : base(createTM, network, clock, random, address, shardLocator, appLocator, backoffCapUs, attemptsPerIncrease, shouldReuseTime, new InversedComparer()) { }
    }
}