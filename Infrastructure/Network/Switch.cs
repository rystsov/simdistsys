using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Transactions.Infrastructure.Network
{
    public class Switch
    {
        private Dictionary<string, IEndpoint> routes = new Dictionary<string, IEndpoint>();
        private HashSet<IEndpoint> networks = new HashSet<IEndpoint>();

        public void RegisterRoute(string address, IEndpoint network)
        {
            routes.Add(address, network);
            networks.Add(network);
        }

        public async Task Run()
        {
            var threads = new List<Task>();
            
            foreach (var network in this.networks)
            {
                threads.Add(ListenAsync(network));
            }

            foreach (var thread in threads)
            {
                await thread;
            }
        }

        private HashSet<string> blocked = new HashSet<string>();

        public void Isolate(string address)
        {
            this.blocked.Add(address);
        }

        public void LiftIsolation(string address)
        {
            this.blocked.Remove(address);
        }

        private async Task ListenAsync(IEndpoint network)
        {
            while (true)
            {
                var message = await network.ReceiveAsync();

                if (this.blocked.Contains(message.Source) || this.blocked.Contains(message.Destination))
                {
                    continue;
                }

                if (routes.ContainsKey(message.Destination))
                {
                    _ = routes[message.Destination].SendAsync(message);
                }
            }
        }
    }
}