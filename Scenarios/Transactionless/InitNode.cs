using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.Transactionless
{
    public class InitNode : Node, IInitNode
    {
        private class InitTx : DbNode.IRWTx
        {
            private readonly Dictionary<string, int> values;

            public InitTx(Dictionary<string, int> values)
            {
                this.values = values;
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                foreach (var key in this.values.Keys)
                {
                    storage.Write(key, new AccountRecord
                    {
                        Value = this.values[key]
                    });
                }

                return "OK";
            }

            public object Clone()
            {
                return new InitTx(this.values.ToDictionary(x => x.Key, x => x.Value));
            }
        }
        
        private readonly Func<string, string> shardLocator;
        
        public InitNode(IEndpoint network, IClock clock, IRandom random, string address, Func<string, string> shardLocator)
            : base(network, clock, random, address)
        {
            this.shardLocator = shardLocator;
        }

        public async Task Run(Dictionary<string, int> accounts)
        {
            var pendingRequests = new HashSet<string>();
            var shardRequests = accounts.GroupBy(x => this.shardLocator(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));

            foreach (var shard in shardRequests.Keys)
            {
                var rid = Guid.NewGuid().ToString();
                pendingRequests.Add(rid);
                
                await this.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                    dest: shard,
                    source: this.address,
                    id: rid,
                    data: new InitTx(shardRequests[shard]),
                    size: Consts.AvgMessageSize
                ));
            }
            
            while (pendingRequests.Count > 0)
            {
                var message = await this.network.ReceiveAsync();

                if (message is DataMessage<DbNode.TxResult> tx)
                {
                    if (!pendingRequests.Contains(tx.ID))
                    {
                        throw new Exception();
                    }
                    else
                    {
                        pendingRequests.Remove(tx.ID);
                    }
                }
                else
                {
                    throw new Exception();
                }
            }
        }

    }
}