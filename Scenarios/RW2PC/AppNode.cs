using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.RW2PC
{
    public class AppNode : Node, IAppNode
    {
        private class CommitTxTx : DbNode.IRWTx
        {
            private readonly string tx;
            private readonly Dictionary<string, int> values;

            public CommitTxTx(string tx, Dictionary<string, int> values)
            {
                this.tx = tx;
                this.values = values;
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                if (storage.Has(this.tx))
                {
                    return (Records.Tx)storage.Read(this.tx);
                }

                var tx = new Records.Tx
                {
                    State = Records.Tx.TxState.Committed,
                    Values = values
                };
                
                storage.Write(this.tx, tx);

                return tx;
            }

            public object Clone()
            {
                return new CommitTxTx(this.tx, this.values.ToDictionary(x => x.Key, x => x.Value));
            }
        }
        
        private class AbortTxTx : DbNode.IRWTx
        {
            private readonly string tx;

            public AbortTxTx(string tx)
            {
                this.tx = tx;
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                if (storage.Has(this.tx))
                {
                    return (Records.Tx)storage.Read(this.tx);
                }

                var tx = new Records.Tx
                {
                    State = Records.Tx.TxState.Aborted,
                    Values = null
                };
                
                storage.Write(this.tx, tx);

                return tx;
            }

            public object Clone()
            {
                return new AbortTxTx(this.tx);
            }
        }

        private class BlockKeysTx : DbNode.IRWTx
        {
            private readonly Dictionary<string, LockOpBlock> blocks;

            public BlockKeysTx(Dictionary<string, LockOpBlock> blocks)
            {
                this.blocks = blocks;
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var result = new Dictionary<string, Records.Account>();

                foreach (var key in this.blocks.Keys)
                {
                    var account = (Records.Account)storage.Read(key);

                    if (account.LockedByTx == this.blocks[key].oldtx)
                    {
                        if (this.blocks[key].value.HasValue)
                        {
                            account.Value = this.blocks[key].value.Value;
                        }
                        account.LockedByTx = null;
                    }

                    if (account.LockedByTx == null)
                    {
                        if (this.blocks[key].isRO)
                        {
                            if (account.ReadLocks.Contains(this.blocks[key].oldtx))
                            {
                                account.ReadLocks.Remove(this.blocks[key].oldtx);
                            }
                            account.ReadLocks.Add(this.blocks[key].newtx);
                            account.TxKeys = null;
                        }
                        else
                        {
                            account.LockedByTx = this.blocks[key].newtx;
                            account.TxKeys = this.blocks[key].keys;
                            account.ReadLocks.Clear();
                        }
                    }

                    storage.Write(key, account);
                    result.Add(key, account);
                }
                
                return new CloneableDictionary<string, Records.Account>(result);
            }

            public object Clone()
            {
                return new BlockKeysTx(this.blocks.ToDictionary(x => x.Key, x => (LockOpBlock)x.Value.Clone()));
            }
        }

        private class ExecuteOrBlockKeysTx : DbNode.IROTx, DbNode.IRWTx
        {
            private readonly Dictionary<string, LockOpBlock> blocks;
            private readonly Func<Dictionary<string, int>, Dictionary<string, int>> change;

            public ExecuteOrBlockKeysTx(Dictionary<string, LockOpBlock> blocks, Func<Dictionary<string, int>, Dictionary<string, int>> change)
            {
                this.blocks = blocks;
                this.change = change;
            }

            public async Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var accounts = blocks.Keys.ToDictionary(x => x, x => (Records.Account)storage.Read(x));

                if (blocks.Values.All(x => x.isRO) && accounts.All(x => x.Value.LockedByTx == null))
                {
                    return new CloneableDictionary<string, Records.Account>(accounts);
                }
                
                return await storage.Execute(this);
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var result = new Dictionary<string, Records.Account>();

                var isClean = true;
                foreach (var key in this.blocks.Keys)
                {
                    var account = (Records.Account)storage.Read(key);

                    if (account.LockedByTx == this.blocks[key].oldtx)
                    {
                        if (this.blocks[key].value.HasValue)
                        {
                            account.Value = this.blocks[key].value.Value;
                        }
                        account.LockedByTx = null;
                    }

                    if (account.LockedByTx == null)
                    {
                        if (this.blocks[key].isRO)
                        {
                            if (account.ReadLocks.Contains(this.blocks[key].oldtx))
                            {
                                account.ReadLocks.Remove(this.blocks[key].oldtx);
                            }
                            account.ReadLocks.Add(this.blocks[key].newtx);
                            account.TxKeys = null;
                        }
                        else
                        {
                            account.LockedByTx = this.blocks[key].newtx;
                            account.TxKeys = this.blocks[key].keys;
                            account.ReadLocks.Clear();
                        }
                    }
                    else
                    {
                        isClean = false;
                    }

                    result.Add(key, account);
                }

                if (isClean)
                {
                    if (this.blocks.Values.All(x => x.isRO))
                    {
                        foreach (var key in this.blocks.Keys)
                        {
                            result[key].ReadLocks.Remove(this.blocks[key].newtx);
                        }
                    }
                    else
                    {
                        var data = result.ToDictionary(x => x.Key, x => x.Value.Value);
                        data = change(data);
                        result = data.ToDictionary(x => x.Key, x => new Records.Account
                        {
                            LockedByTx = null,
                            TxKeys = null,
                            ReadLocks = new HashSet<string>(),
                            Value = x.Value
                        });
                    }
                }
                
                foreach (var key in result.Keys)
                {
                    storage.Write(key, result[key]);
                }
                
                return new CloneableDictionary<string, Records.Account>(result);
            }

            public object Clone()
            {
                return new ExecuteOrBlockKeysTx(this.blocks.ToDictionary(x => x.Key, x => (LockOpBlock)x.Value.Clone()), change);
            }
        }

        private class Released : DbNode.Marker<Released> {}
        private class ReleaseIgnored : DbNode.Marker<ReleaseIgnored> {}

        private class ReleaseKeysTx : DbNode.IRWTx, DbNode.IROTx
        {
            private readonly Dictionary<string, LockOpRelease> releases;

            public ReleaseKeysTx(Dictionary<string, LockOpRelease> releases)
            {
                this.releases = releases;
            }

            public Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var hasAlreadyReleased = false;
                
                foreach (var key in this.releases.Keys)
                {
                    var account = (Records.Account)storage.Read(key);
                    var op = this.releases[key];

                    if (account.LockedByTx != op.tx && !account.ReadLocks.Contains(op.tx))
                    {
                        hasAlreadyReleased = true;
                    }
                }

                _ = storage.Execute(this);

                if (hasAlreadyReleased)
                {
                    return Task.FromResult<ICloneable>(new ReleaseIgnored());
                }
                else
                {
                    return Task.FromResult<ICloneable>(new Released());
                }
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                foreach (var key in this.releases.Keys)
                {
                    var account = (Records.Account)storage.Read(key);
                    var op = this.releases[key];

                    if (account.LockedByTx == op.tx)
                    {
                        account.LockedByTx = null;
                        account.TxKeys = null;
                        if (op.value.HasValue)
                        {
                            account.Value = op.value.Value;
                        }
                        storage.Write(key, account);
                    }
                    else if (account.ReadLocks.Contains(op.tx))
                    {
                        account.ReadLocks.Remove(op.tx);
                        storage.Write(key, account);
                    }
                }

                return null;
            }

            public object Clone()
            {
                return new ReleaseKeysTx(this.releases.ToDictionary(x => x.Key, x => x.Value));
            }
        }

        private class LockOpBlock : ICloneable
        {
            public string oldtx;
            public bool isRO;
            public string newtx;
            public int? value;
            public List<string> keys;

            public object Clone()
            {
                return new LockOpBlock
                {
                    oldtx = this.oldtx,
                    newtx = this.newtx,
                    value = this.value,
                    isRO = this.isRO,
                    keys = new List<string>(this.keys)
                };
            }

            public override string ToString()
            {
                var v = value.HasValue ? "" + value.Value : "null";
                return $"{oldtx ?? "null"} -> {newtx} value: {v}";
            }
        }
        
        private class LockOpRelease : ICloneable
        {
            public string tx;
            public int? value;

            public object Clone()
            {
                return new LockOpRelease
                {
                    tx = this.tx,
                    value = this.value
                };
            }
        }

        private class BlockedKeys
        {
            public string newtx;
            public bool hasExecuted = false;
            public Dictionary<string, int> read;
        }

        protected readonly bool debug = false;
        private readonly long backoffCapUs;
        private readonly int attemptsPerIncrease;
        protected readonly Func<string, string> shardLocator;
        
        public AppNode(IEndpoint network, IClock clock, IRandom random, string address, Func<string, string> shardLocator, long backoffCapUs, int attemptsPerIncrease)
            : base(network, clock, random, address)
        {
            this.shardLocator = shardLocator;
            this.backoffCapUs = backoffCapUs;
            this.attemptsPerIncrease = attemptsPerIncrease;
        }

        public async Task Run()
        {
            while (true)
            {
                var message = await this.network.ReceiveAsync();

                if (message is DataMessage<AppMessages.TransferTx> txt)
                {
                    _ = this.Process(txt);
                }
                else if (message is DataMessage<AppMessages.ReadTx> txr)
                {
                    _ = this.Process(txr);
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
    
        protected virtual async Task Process(DataMessage<AppMessages.TransferTx> tx)
        {
            try
            {
                var mytx = Guid.NewGuid().ToString();
                var plan = new Dictionary<string, LockOpBlock>();
                var keys = new[] {tx.Data.Donor, tx.Data.Recipient};
                foreach (var key in keys)
                {
                    plan[key] = new LockOpBlock
                    {
                        oldtx = null,
                        newtx = mytx,
                        value = null,
                        isRO = false,
                        keys = keys.ToList()
                    };
                }
                await this.Execute(mytx, plan, Transfer, Reply, tx.Source);
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            Dictionary<string, int> Transfer(Dictionary<string, int> accounts)
            {
                var copy = accounts.ToDictionary(x => x.Key, x => x.Value);
                copy[tx.Data.Donor] -= tx.Data.Amount;
                copy[tx.Data.Recipient] += tx.Data.Amount;
                return copy;
            }

            async Task Reply(Dictionary<string, int> accounts)
            {
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
        }

        protected virtual async Task Process(DataMessage<AppMessages.ReadTx> tx)
        {
            try
            {
                var mytx = Guid.NewGuid().ToString();
                var plan = new Dictionary<string, LockOpBlock>();
                var keys = tx.Data.Accounts;
                foreach (var key in keys)
                {
                    plan[key] = new LockOpBlock
                    {
                        oldtx = null,
                        newtx = mytx,
                        value = null,
                        keys = keys,
                        isRO = true
                    };
                }
                await this.ExecuteRO(mytx, plan, Reply, tx.Source);
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            async Task Reply(Dictionary<string, int> accounts)
            {
                var result = new AppMessages.TxOk();
                foreach (var key in tx.Data.Accounts)
                {
                    result.Accounts[key] = accounts[key];
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

        private async Task Execute(string mytx, Dictionary<string, LockOpBlock> plan, Func<Dictionary<string, int>, Dictionary<string, int>> change, Func<Dictionary<string, int>, Task> reply, string clientId)
        {
            if (debug)
            {
                Console.WriteLine("###########################################################");
                Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} started");
                Console.WriteLine("\tplan:");
                foreach (var key in plan.Keys)
                {
                    Console.WriteLine($"\t\t{key}: {plan[key]}");
                }
                Console.WriteLine();
            }

            var keys = plan.Keys.ToList();


            BlockedKeys blocked;

            // if all the keys are on the same shard try short cut
            if (keys.GroupBy(shardLocator).Count() == 1)
            {
                blocked = await this.ExecuteOrBlockKeys(shardLocator(keys.First()), mytx, plan, change, clientId);
                if (blocked.hasExecuted)
                {
                    await reply(blocked.read);
                }
                return;
            }
            else
            {
                blocked = await this.BlockKeys(mytx, plan, clientId);
            }

            mytx = blocked.newtx;
            if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} blocked");
            
            // TODO: if RO then unblock else commit
            
            var read = change(blocked.read);
            var tx = await this.CommitTx(mytx, read);
            if (tx.State == Records.Tx.TxState.Aborted)
            {
                if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} aborted");
                var newtx = Guid.NewGuid().ToString();
                if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} -> {newtx}");
                foreach (var key in keys)
                {
                    plan[key] = new LockOpBlock
                    {
                        keys = keys,
                        oldtx = mytx,
                        newtx = newtx,
                        value = null
                    };
                }
                await Execute(newtx, plan, change, reply, clientId);
            }
            else
            {
                if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} committed");
                
                await reply(read);

                var releasesByShards = keys.GroupBy(x => this.shardLocator(x)).ToDictionary(x => x.Key, x => x.ToList());

                foreach (var shard in releasesByShards.Keys)
                {
                    _ = this.ReleaseKeys(shard, releasesByShards[shard].ToDictionary(x => x, x => new LockOpRelease { tx = mytx, value = read[x] }));
                }
            }
        }
        
        private async Task ExecuteRO(string mytx, Dictionary<string, LockOpBlock> plan, Func<Dictionary<string, int>, Task> reply, string clientId)
        {
            while (true)
            {
                if (debug)
                {
                    Console.WriteLine("###########################################################");
                    Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} started");
                    Console.WriteLine("\tplan:");
                    foreach (var key in plan.Keys)
                    {
                        Console.WriteLine($"\t\t{key}: {plan[key]}");
                    }
                    Console.WriteLine();
                }

                var keys = plan.Keys.ToList();


                BlockedKeys blocked;

                // if all the keys are on the same shard try short cut
                if (keys.GroupBy(shardLocator).Count() == 1)
                {
                    blocked = await this.ExecuteOrBlockKeys(shardLocator(keys.First()), mytx, plan, null, clientId);
                    if (blocked.hasExecuted)
                    {
                        await reply(blocked.read);
                        return;
                    }
                }
                else
                {
                    blocked = await this.BlockKeys(mytx, plan, clientId);
                }

                mytx = blocked.newtx;
                if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} blocked");


                var releasesByShards = keys.GroupBy(x => this.shardLocator(x)).ToDictionary(x => x.Key, x => x.ToList());
                var releasing = new List<Task<bool>>();

                foreach (var shard in releasesByShards.Keys)
                {
                    releasing.Add(this.ReleaseKeys(shard, releasesByShards[shard].ToDictionary(x => x, _ => new LockOpRelease { tx = mytx, value = null })));
                }

                var released = true;
                foreach (var request in releasing)
                {
                    released &= await request;
                }

                if (released)
                {
                    await reply(blocked.read);
                    return;
                }

                var newtx = Guid.NewGuid().ToString();
                if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} -> {newtx} retry");
                mytx = newtx;
            }
        }

        private async Task<BlockedKeys> BlockKeys(string mytx, Dictionary<string, LockOpBlock> plan, string clientId)
        {
            var attempts = 0;
            var backoff = 1;
            while (true)
            {
                if (attempts > 1)
                {
                    if (backoff < this.backoffCapUs)
                    {
                        if (attempts % attemptsPerIncrease == 0)
                        {
                            backoff *= 2;
                        }
                    }
                    await this.clock.Delay(new Microsecond((ulong)this.random.Next(backoff)));
                }
                attempts++;
                
                if (debug)
                {
                    Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} blocking");
                    Console.WriteLine("\tplan:");
                    foreach (var key in plan.Keys)
                    {
                        Console.WriteLine($"\t\t{key}: {plan[key]}");
                    }
                }
                
                var blocking = new Dictionary<string, Task<Dictionary<string,Records.Account>>>();
                
                var planByShards = plan.GroupBy(x => shardLocator(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));
                
                foreach (var shard in planByShards.Keys)
                {
                    blocking.Add(shard, this.BlockKeys(shard, planByShards[shard]));
                }

                // flat the responce

                var read = new Dictionary<string, Records.Account>();

                foreach (var shard in blocking.Keys)
                {
                    var accounts = await blocking[shard];

                    foreach (var key in accounts.Keys)
                    {
                        var account = accounts[key];
                        read.Add(key, account);
                    }
                }

                if (read.All(x => x.Value.LockedByTx == mytx || (x.Value.LockedByTx == null && x.Value.ReadLocks.Contains(mytx))))
                {
                    return new BlockedKeys
                    {
                        hasExecuted = false,
                        newtx = mytx,
                        read = read.ToDictionary(x => x.Key, x => x.Value.Value)
                    };
                }

                // else:
                //   I'm RO and saw W lock
                //   I'm W and saw W lock

                // else clean up

                var newtx = Guid.NewGuid().ToString();
                
                plan = await CleanUp(mytx, newtx, read, plan, clientId);
                mytx = newtx;

                if (debug) Console.WriteLine($"{clientId} {this.clock.Now.value} Retry Block");
            }
        }

        private async Task<BlockedKeys> ExecuteOrBlockKeys(string shard, string mytx, Dictionary<string, LockOpBlock> plan, Func<Dictionary<string, int>, Dictionary<string, int>> change, string clientId)
        {
            var attempts = 0;
            var backoff = 1;
            while (true)
            {
                if (attempts > 1)
                {
                    if (backoff < this.backoffCapUs)
                    {
                        if (attempts % attemptsPerIncrease == 0)
                        {
                            backoff *= 2;
                        }
                    }
                    await this.clock.Delay(new Microsecond((ulong)this.random.Next(backoff)));
                }
                attempts++;

                if (debug)
                {
                    Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} blocking");
                    Console.WriteLine("\tplan:");
                    foreach (var key in plan.Keys)
                    {
                        Console.WriteLine($"\t\t{key}: {plan[key]}");
                    }
                }

                var read = await ExecuteOrBlockKeys(shard, plan, change);

                if (read.All(x => x.Value.LockedByTx == null))
                {
                    return new BlockedKeys
                    {
                        hasExecuted = true,
                        newtx = mytx,
                        read = read.ToDictionary(x => x.Key, x => x.Value.Value)
                    };
                }

                if (read.All(x => x.Value.LockedByTx == mytx))
                {
                    return new BlockedKeys
                    {
                        hasExecuted = false,
                        newtx = mytx,
                        read = read.ToDictionary(x => x.Key, x => x.Value.Value)
                    };
                }

                // else clean up

                var newtx = Guid.NewGuid().ToString();
                
                plan = await CleanUp(mytx, newtx, read, plan, clientId);
                mytx = newtx;

                if (debug) Console.WriteLine($"{clientId} {this.clock.Now.value} Retry Block");
            }
        }

        private async Task<Dictionary<string, LockOpBlock>> CleanUp(string mytx, string newtx, Dictionary<string, Records.Account> read, Dictionary<string, LockOpBlock> plan, string clientId)
        {
            var keys = plan.Keys.ToList();
            var conflicts = new Dictionary<string, List<Records.Account>>();

            foreach (var key in read.Keys)
            {
                var account = read[key];
                if (account.LockedByTx != null && account.LockedByTx != mytx)
                {
                    if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} conflicts with {account.LockedByTx} over {key}");
                    if (!conflicts.ContainsKey(account.LockedByTx))
                    {
                        conflicts.Add(account.LockedByTx, new List<Records.Account>());
                    }
                    conflicts[account.LockedByTx].Add(account);
                }
            }

            var aborting = new Dictionary<string, Task<Records.Tx>>();
            foreach (var tx in conflicts.Keys)
            {
                if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} aborting {tx}");
                aborting.Add(tx, AbortTx(tx));
            }

            if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} -> {newtx}");

            foreach (var key in keys)
            {
                plan[key] = new LockOpBlock
                {
                    oldtx = mytx,
                    newtx = newtx,
                    isRO = plan[key].isRO,
                    value = read[key].Value,
                    keys = keys
                };
            }

            var release = new Dictionary<string, LockOpRelease>();
            foreach (var tx in conflicts.Keys)
            {
                var opponent = await aborting[tx];
                if (opponent.State == Records.Tx.TxState.Committed)
                {
                    if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} cant abort comitted {newtx}");
                    foreach (var key in opponent.Values.Keys)
                    {
                        if (plan.ContainsKey(key))
                        {
                            plan[key] = new LockOpBlock
                            {
                                oldtx = tx,
                                newtx = newtx,
                                keys = keys,
                                isRO = plan[key].isRO,
                                value = opponent.Values[key]
                            };
                        }
                        else
                        {
                            if (!release.ContainsKey(key))
                            {
                                release.Add(key, new LockOpRelease
                                {
                                    tx = tx,
                                    value = opponent.Values[key]
                                });
                            }
                        }
                    }
                }
                else if (opponent.State == Records.Tx.TxState.Aborted)
                {
                    if (debug) Console.WriteLine($"[{clientId}] {this.clock.Now.value} {mytx} aborted {newtx}");
                    foreach (var key in conflicts[tx].First().TxKeys)
                    {
                        if (plan.ContainsKey(key))
                        {
                            if (read[key].LockedByTx == tx)
                            {
                                plan[key] = new LockOpBlock
                                {
                                    oldtx = tx,
                                    newtx = newtx,
                                    keys = keys,
                                    value = null,
                                    isRO = plan[key].isRO
                                };
                            }
                            else if (read[key].LockedByTx == mytx || (read[key].LockedByTx == null && read[key].ReadLocks != null && read[key].ReadLocks.Contains(mytx)))
                            {
                                plan[key] = new LockOpBlock
                                {
                                    oldtx = mytx,
                                    newtx = newtx,
                                    keys = keys,
                                    value = null,
                                    isRO = plan[key].isRO
                                };
                            }
                        }
                        else
                        {
                            // TODO: did I forget to put it into release?!
                        }
                    }
                }
                else
                {
                    throw new Exception();
                }
            }

            var releasesByShards = release.GroupBy(x => this.shardLocator(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));

            foreach (var shard in releasesByShards.Keys)
            {
                _ = this.ReleaseKeys(shard, releasesByShards[shard]);
            }

            return plan;
        }

        private async Task<Dictionary<string, Records.Account>> BlockKeys(string shard, Dictionary<string, LockOpBlock> blocks)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                dest: shard,
                source: this.address,
                id: rid,
                data: new BlockKeysTx(blocks),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }

        private async Task<Dictionary<string, Records.Account>> ExecuteOrBlockKeys(string shard, Dictionary<string, LockOpBlock> blocks, Func<Dictionary<string, int>, Dictionary<string, int>> change)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.address,
                id: rid,
                data: new ExecuteOrBlockKeysTx(blocks, change),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }

        private async Task<bool> ReleaseKeys(string shard, Dictionary<string, LockOpRelease> releases)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.address,
                id: rid,
                data: new ReleaseKeysTx(releases),
                size: Consts.AvgMessageSize
            ));

            var result = ((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result;

            if (result is Released)
            {
                return true;
            }
            else if (result is ReleaseIgnored)
            {
                return false;
            }
            else
            {
                var message = $"Unknown type: {result.GetType().FullName}";
                Console.WriteLine(message);
                throw new Exception(message);
            }
        }

        private async Task<Records.Tx> AbortTx(string tx)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                dest: this.shardLocator(tx),
                source: this.address,
                id: rid,
                data: new AbortTxTx(tx),
                size: Consts.AvgMessageSize
            ));
            return (Records.Tx)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result;
        }

        private async Task<Records.Tx> CommitTx(string tx, Dictionary<string, int> values)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.replies.Add(rid, tcs);
            await this.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                dest: this.shardLocator(tx),
                source: this.address,
                id: rid,
                data: new CommitTxTx(tx, values),
                size: Consts.AvgMessageSize
            ));
            return (Records.Tx)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result;
        }
    }
}