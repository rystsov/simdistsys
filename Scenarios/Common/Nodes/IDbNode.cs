using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.Common.Nodes
{
    public delegate IDbNode DbNodeFactory(IEndpoint network, IClock clock, IRandom random, string address, Func<string, string> shardLocator, SSDSpec io);
    
    public interface IDbNode
    {
        Task Run();
    }
}