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
    public class AppNode : Node, IAppNode
    {
        private class ReadTx : DbNode.IROTx
        {
            private readonly HashSet<string> keys;

            public ReadTx(HashSet<string> keys)
            {
                this.keys = keys;
            }
            
            public Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var result = new Dictionary<string, int>();
                
                foreach (var key in this.keys)
                {
                    result.Add(key, ((AccountRecord)storage.Read(key)).Value);
                }

                return Task.FromResult<ICloneable>(new CloneableDictionary<string, int>(result));
            }

            public object Clone()
            {
                return new ReadTx(new HashSet<string>(this.keys));
            }
        }
        
        private class WriteTx : DbNode.IRWTx
        {
            private readonly Dictionary<string, int> values;

            public WriteTx(Dictionary<string, int> values)
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
                return new WriteTx(this.values.ToDictionary(x => x.Key, x => x.Value));
            }
        }

        private class TransferTx : DbNode.IRWTx
        {
            private readonly string donor;
            private readonly string recipient;
            private readonly int delta;

            public TransferTx(string donor, string recipient, int delta)
            {
                this.donor = donor;
                this.recipient = recipient;
                this.delta = delta;
            }

            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var result = new Dictionary<string, int>();
                
                result[donor] = ((AccountRecord)storage.Read(donor)).Value - delta;
                result[recipient] = ((AccountRecord)storage.Read(recipient)).Value + delta;
                
                foreach (var key in result.Keys)
                {
                    storage.Write(key, new AccountRecord
                    {
                        Value = result[key]
                    });
                }

                return new CloneableDictionary<string, int>(result);
            }

            public object Clone()
            {
                return new TransferTx(this.donor, this.recipient, this.delta);
            }
        }

        private readonly Func<string, string> shardLocator;
        
        public AppNode(IEndpoint network, IClock clock, IRandom random, string address, Func<string, string> shardLocator)
            : base(network, clock, random, address)
        {
            this.shardLocator = shardLocator;
        }

        public async Task Run()
        {
            while (true)
            {
                var message = await this.network.ReceiveAsync();

                if (message is DataMessage<AppMessages.TransferTx> txt)
                {
                    _ = this.Execute(txt);
                }
                else if (message is DataMessage<AppMessages.ReadTx> txr)
                {
                    _ = this.Execute(txr);
                }
                else if (message is DataMessage<DbNode.TxResult> ltxr)
                {
                    if (this.replies.ContainsKey(ltxr.ID))
                    {
                        var pr = this.replies[ltxr.ID];
                        this.replies.Remove(ltxr.ID);
                        pr.SetResult(ltxr);
                    }
                    else
                    {
                        throw new Exception();
                    }
                }
                else
                {
                    Console.WriteLine($"{nameof(AppNode)}: Unexpected message: {message.GetType().FullName}");
                    throw new Exception();
                }
            }
        }

        private async Task<Dictionary<string, int>> Read(string shard, HashSet<string> keys)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.address,
                id: rid,
                data: new ReadTx(keys),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string,int>)((DataMessage<DbNode.TxResult>)await tcs.Task).Data.Result).data;
        }

        private async Task<Dictionary<string, int>> Read(HashSet<string> accounts)
        {
            var shardRequests = accounts.GroupBy(x => this.shardLocator(x)).ToDictionary(x => x.Key, x => x.ToHashSet());

            var requests = new List<Task<Dictionary<string, int>>>();
            foreach (var shard in shardRequests.Keys)
            {
                requests.Add(this.Read(shard, shardRequests[shard]));
            }

            var result = new Dictionary<string, int>();

            foreach (var request in requests)
            {
                foreach (var item in await request)
                {
                    result.Add(item.Key, item.Value);
                }
            }

            return result;
        }

        private async Task Write(string shard, Dictionary<string, int> accounts)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                dest: shard,
                source: this.address,
                id: rid,
                data: new WriteTx(accounts),
                size: Consts.AvgMessageSize
            ));
            await tcs.Task;
        }

        private async Task Write(Dictionary<string, int> accounts)
        {
            var shardRequests = accounts.GroupBy(x => this.shardLocator(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));

            var requests = new List<Task>();
            foreach (var shard in shardRequests.Keys)
            {
                requests.Add(this.Write(shard, shardRequests[shard]));
            }

            foreach (var request in requests)
            {
                await request;
            }
        }

        private async Task<Dictionary<string, int>> Transfer(string shard, DataMessage<AppMessages.TransferTx> tx)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                dest: shard,
                source: this.address,
                id: rid,
                data: new TransferTx(tx.Data.Donor, tx.Data.Recipient, tx.Data.Amount),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string,int>)((DataMessage<DbNode.TxResult>)await tcs.Task).Data.Result).data;
        }

        private async Task Execute(DataMessage<AppMessages.TransferTx> tx)
        {
            if (tx.Data.Donor == tx.Data.Recipient)
            {
                var message = $"donor is same as recipient ({tx.Data.Donor})";
                Console.WriteLine(message);
                throw new Exception(message);
            }

            Dictionary<string, int> accounts;

            if (shardLocator(tx.Data.Donor) == shardLocator(tx.Data.Recipient))
            {
                accounts = await Transfer(shardLocator(tx.Data.Donor), tx);
            }
            else
            {
                accounts = await Read(new HashSet<string> { tx.Data.Donor, tx.Data.Recipient });

                accounts[tx.Data.Donor]-=tx.Data.Amount;
                accounts[tx.Data.Recipient]+=tx.Data.Amount;

                await Write(accounts);
            }

            var result = new AppMessages.TxOk();
            result.Accounts[tx.Data.Donor] = accounts[tx.Data.Donor];
            result.Accounts[tx.Data.Recipient] = accounts[tx.Data.Recipient];

            await this.network.SendAsync(new DataMessage<AppMessages.TxOk>(
                dest: tx.Source,
                source: this.address,
                id: tx.ID,
                data: result,
                size: Consts.AvgMessageSize
            ));
        }
    
        private async Task Execute(DataMessage<AppMessages.ReadTx> tx)
        {
            var read = await Read(new HashSet<string>(tx.Data.Accounts));

            var result = new AppMessages.TxOk();
            foreach(var account in read.Keys)
            {
                result.Accounts.Add(account, read[account]);
            }

            await this.network.SendAsync(new DataMessage<AppMessages.TxOk>(
                dest: tx.Source,
                source: this.address,
                id: tx.ID,
                data: result,
                size: Consts.AvgMessageSize
            ));
        }
    }
}