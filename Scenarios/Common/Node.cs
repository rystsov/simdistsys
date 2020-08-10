using Transactions.Infrastructure.Network;
using Transactions.Infrastructure;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Transactions.Scenarios.Common
{
    public class Node
    {
        public readonly string address;
        public readonly IEndpoint network;
        public readonly IRandom random;
        public readonly IClock clock;
        public readonly Dictionary<string, TaskCompletionSource<IMessage>> replies = new Dictionary<string, TaskCompletionSource<IMessage>>();

        public Node(IEndpoint network, IClock clock, IRandom random, string address)
        {
            this.address = address;
            this.network = network;
            this.random = random;
            this.clock = clock;
        }
    }
}