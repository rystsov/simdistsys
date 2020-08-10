using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.Mem
{
    public delegate IMaterializedLocksTM MaterializedLocksTMFactory(Node node, Func<string, string> shardLocator, bool useBackoff, long backoffCapUs, int attemptsPerIncrease);
    
    public interface IMaterializedLocksTM
    {
        Task<Dictionary<string, int>> Read(HashSet<string> keys, string clientId);
        Task<Dictionary<string, int>> Change(HashSet<string> keys, Func<Dictionary<string, int>, Dictionary<string, int>> change, string clientId);
    }
}