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

namespace Transactions.Scenarios.Mem.Rnd
{
    public class MemRndParallel2PCAppNode : Node, IAppNode
    {
        protected readonly bool debug = false;
        protected readonly Func<string, string> shardLocator2;
        protected readonly Func<string, string> appLocator;

        private readonly Parallel2PCCore core;
        private long backoffCapUs;
        private int attemptsPerIncrease;
        
        public MemRndParallel2PCAppNode(IEndpoint network, IClock clock, IRandom random, string address, Func<string, string> shardLocator, Func<string, string> appLocator, long backoffCapUs, int attemptsPerIncrease)
            : base(network, clock, random, address)
        {
            this.core = new Parallel2PCCore(this, shardLocator, false, 0, 0);
            this.shardLocator2 = shardLocator;
            this.appLocator = appLocator;
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
                    var keys = new[] { txt.Data.Donor, txt.Data.Recipient };

                    if (keys.Any(key => this.appLocator(key) == this.address))
                    {
                        var locks = new Dictionary<string, LockType>()
                        {
                            { txt.Data.Donor, LockType.W },
                            { txt.Data.Recipient, LockType.W }
                        };
                        
                        _ = this.Coord2(locks, () => this.Execute(txt));
                    }
                    else
                    {
                        await this.network.SendAsync(new DataMessage<AppMessages.TransferTx>(
                            dest: this.appLocator(txt.Data.Donor),
                            source: txt.Source,
                            id: txt.ID,
                            data: txt.Data,
                            size: txt.Size
                        ));
                    }
                }
                else if (message is DataMessage<AppMessages.ReadTx> txr)
                {
                    if (txr.Data.Accounts.Any(key => this.appLocator(key) == this.address))
                    {
                        var locks = txr.Data.Accounts.ToDictionary(x => x, _ => LockType.R);
                        
                        _ = this.Coord2(locks, () => this.Execute(txr));
                    }
                    else
                    {
                        await this.network.SendAsync(new DataMessage<AppMessages.ReadTx>(
                            dest: this.appLocator(txr.Data.Accounts[0]),
                            source: txr.Source,
                            id: txr.ID,
                            data: txr.Data,
                            size: txr.Size
                        ));
                    }
                }
                else if (message is TryMemLockCmd tml)
                {
                    _ = tml.ExecuteAndReply(this);
                }
                else if (message is ReleaseMemLockCmd rml)
                {
                    _ = rml.ExecuteAndReply(this);
                }
                else if (this.replies.ContainsKey(message.ID))
                {
                    var pr = this.replies[message.ID];
                    this.replies.Remove(message.ID);
                    pr.SetResult(message);
                }
                else
                {
                    Console.WriteLine($"{nameof(MemRndParallel2PCAppNode)}: Unexpected message: {message.GetType().FullName}");
                    throw new Exception();
                }
            }
        }

        ////////////////////////////////////////////////////////

        private enum LockType { R, W }

        private class PendingTx
        {
            public string tx;
            public LockType lockType;
            public TaskCompletionSource<bool> ready;
        }

        private readonly Dictionary<string, string> pendingMinRequestByKey = new Dictionary<string, string>();
        private readonly Dictionary<string, Queue<PendingTx>> pendingRequestsByKey = new Dictionary<string, Queue<PendingTx>>();
        private readonly Dictionary<string, HashSet<string>> readLocksByKey = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<string, string> writeLocksByKey = new Dictionary<string, string>();

        private class LocksAccepted : DbNode.Marker<LocksAccepted> {}
        private class LocksRejected : DbNode.Marker<LocksRejected> {}
        private class Done : DbNode.Marker<Done> {}

        private class TryMemLockCmd : Letter<TryMemLockCmd>
        {
            public Dictionary<string, LockType> locks;
            public string tx;

            public async Task<ICloneable> Execute(MemRndParallel2PCAppNode app)
            {
                var abort = false;
                foreach (var key in this.locks.Keys)
                {
                    abort |= app.pendingMinRequestByKey.ContainsKey(key) && app.pendingMinRequestByKey[key].CompareTo(this.tx) < 0;
                }

                if (abort)
                {
                    return new LocksRejected();
                }

                var waiting = new List<Task>();
                foreach (var key in this.locks.Keys)
                {
                    if (app.pendingMinRequestByKey.ContainsKey(key))
                    {
                        app.pendingMinRequestByKey[key] = this.tx;
                        if (this.locks[key] == LockType.W)
                        {
                            var tcs = new TaskCompletionSource<bool>();
                            if (!app.pendingRequestsByKey.ContainsKey(key))
                            {
                                app.pendingRequestsByKey.Add(key, new Queue<PendingTx>());
                            }
                            app.pendingRequestsByKey[key].Enqueue(new PendingTx
                            {
                                lockType = this.locks[key],
                                ready = tcs,
                                tx = this.tx
                            });
                            waiting.Add(tcs.Task);
                        }
                        else
                        {
                            if (app.writeLocksByKey.ContainsKey(key) || (app.pendingRequestsByKey.ContainsKey(key) && app.pendingRequestsByKey[key].Any()))
                            {
                                var tcs = new TaskCompletionSource<bool>();
                                if (!app.pendingRequestsByKey.ContainsKey(key))
                                {
                                    app.pendingRequestsByKey.Add(key, new Queue<PendingTx>());
                                }
                                app.pendingRequestsByKey[key].Enqueue(new PendingTx
                                {
                                    lockType = this.locks[key],
                                    ready = tcs,
                                    tx = this.tx
                                });
                                waiting.Add(tcs.Task);
                            }
                            else
                            {
                                app.readLocksByKey[key].Add(this.tx);
                            }
                        }
                    }
                    else
                    {
                        app.pendingMinRequestByKey.Add(key, this.tx);
                        if (this.locks[key] == LockType.W)
                        {
                            app.writeLocksByKey.Add(key, this.tx);
                        }
                        else
                        {
                            if (!app.readLocksByKey.ContainsKey(key))
                            {
                                app.readLocksByKey.Add(key, new HashSet<string>());
                            }
                            app.readLocksByKey[key].Add(this.tx);
                        }
                    }
                }
                foreach (var task in waiting)
                {
                    await task;
                }

                return new LocksAccepted();
            }
            
            public async Task ExecuteAndReply(MemRndParallel2PCAppNode app)
            {
                try
                {
                    var result = await this.Execute(app);
                    await app.network.SendAsync(new DataMessage<ICloneable>(
                        dest: this.Source,
                        source: this.Destination,
                        id: this.ID,
                        data: result,
                        size: this.Size
                    ));
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
            
            public override object Clone()
            {
                var copy = (TryMemLockCmd)base.Clone();
                copy.locks = this.locks.ToDictionary(x => x.Key, x=> x.Value);
                copy.tx = tx;
                return copy;
            }
        }
        
        private class ReleaseMemLockCmd : Letter<ReleaseMemLockCmd>
        {
            public HashSet<string> keys;
            public string tx;

            public Task<ICloneable> Execute(MemRndParallel2PCAppNode app)
            {
                var ready = new List<TaskCompletionSource<bool>>();
                
                foreach (var key in this.keys)
                {
                    if (app.readLocksByKey.ContainsKey(key) && app.readLocksByKey[key].Contains(this.tx))
                    {
                        app.readLocksByKey[key].Remove(this.tx);
                        if (app.readLocksByKey[key].Count == 0)
                        {
                            app.readLocksByKey.Remove(key);
                        }
                    }
                    else if (app.writeLocksByKey.ContainsKey(key) && app.writeLocksByKey[key] == this.tx)
                    {
                        app.writeLocksByKey.Remove(key);
                    }
                    else
                    {
                        Console.WriteLine($"Unexpected key (not a read lock, not a write lock): {key}");
                        throw new Exception($"Unexpected key (not a read lock, not a write lock): {key}");
                    }

                    if (app.pendingRequestsByKey.ContainsKey(key))
                    {
                        while (app.pendingRequestsByKey[key].Any() && app.pendingRequestsByKey[key].Peek().lockType == LockType.R)
                        {
                            var tx = app.pendingRequestsByKey[key].Dequeue();
                            if (!app.readLocksByKey.ContainsKey(key))
                            {
                                app.readLocksByKey.Add(key, new HashSet<string>());
                            }
                            app.readLocksByKey[key].Add(tx.tx);
                            ready.Add(tx.ready);
                        }

                        if (app.pendingRequestsByKey[key].Any())
                        {
                            if (!app.readLocksByKey.ContainsKey(key) || !app.readLocksByKey[key].Any())
                            {
                                var tx = app.pendingRequestsByKey[key].Dequeue();
                                if (tx.lockType != LockType.W)
                                {
                                    Console.WriteLine("Impossible state, only W lock is expected");
                                    throw new Exception("Impossible state, only W lock is expected");
                                }

                                if (app.writeLocksByKey.ContainsKey(key))
                                {
                                    Console.WriteLine("Impossible state, app.writeLocksByKey must be empty");
                                    throw new Exception("Impossible state, app.writeLocksByKey must be empty");
                                }

                                app.writeLocksByKey.Add(key, tx.tx);
                                ready.Add(tx.ready);
                            }
                        }

                        if (!app.pendingRequestsByKey[key].Any())
                        {
                            app.pendingRequestsByKey.Remove(key);
                        }
                    }

                    if (!app.readLocksByKey.ContainsKey(key) && !app.writeLocksByKey.ContainsKey(key))
                    {
                        if (app.pendingMinRequestByKey.ContainsKey(key))
                        {
                            app.pendingMinRequestByKey.Remove(key);
                        }
                    }
                }

                foreach (var x in ready)
                {
                    x.SetResult(true);
                }

                return Task.FromResult<ICloneable>(new Done());
            }
            
            public async Task ExecuteAndReply(MemRndParallel2PCAppNode app)
            {
                try
                {
                    var result = await this.Execute(app);
                    await app.network.SendAsync(new DataMessage<ICloneable>(
                        dest: this.Source,
                        source: this.Destination,
                        id: this.ID,
                        data: result,
                        size: this.Size
                    ));
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }

            public override object Clone()
            {
                var copy = (ReleaseMemLockCmd)base.Clone();
                copy.tx = this.tx;
                copy.keys = this.keys.ToHashSet();
                return copy;
            }
        }

        private async Task Coord2(Dictionary<string, LockType> keys, Func<Task> work)
        {
            try
            {
                var tx = Guid.NewGuid().ToString();
                var locksByApp = keys.GroupBy(x => this.appLocator(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));
                
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

                    var lockRequests = new Dictionary<string, Task<bool>>();
                    foreach (var app in locksByApp.Keys)
                    {
                        lockRequests.Add(app, TryMemLock(app, tx, locksByApp[app]));
                    }

                    var takenLocks = new List<string>();
                    foreach (var app in lockRequests.Keys)
                    {
                        if (await lockRequests[app])
                        {
                            takenLocks.Add(app);
                        }
                    }

                    if (takenLocks.Count != lockRequests.Count)
                    {
                        var releaseRequests = new List<Task>();
                        
                        foreach (var app in takenLocks)
                        {
                            releaseRequests.Add(ReleaseMemLock(app, tx, locksByApp[app].Keys.ToHashSet()));
                        }
                        
                        foreach (var release in releaseRequests)
                        {
                            await release;
                        }

                        tx = Guid.NewGuid().ToString();
                    }
                    else
                    {
                        await work();
                        foreach (var app in takenLocks)
                        {
                            _ = ReleaseMemLock(app, tx, locksByApp[app].Keys.ToHashSet());
                        }
                        return;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
        
        private async Task<bool> TryMemLock(string shard, string tx, Dictionary<string, LockType> locks)
        {
            var rid = Guid.NewGuid().ToString();
            var cmd = new TryMemLockCmd
            {
                tx = tx,
                locks = locks,
                Destination = shard,
                Source = this.address,
                ID = rid,
                Size = Consts.AvgMessageSize
            };
            ICloneable result;
            if (this.address == shard)
            {
                result = await cmd.Execute(this);
            }
            else
            {
                var tcs = new TaskCompletionSource<IMessage>();
                this.replies.Add(rid, tcs);
                await this.network.SendAsync(cmd);
                result = ((DataMessage<ICloneable>)(await tcs.Task)).Data;
            }
            if (result is LocksAccepted)
            {
                return true;
            }
            else if (result is LocksRejected)
            {
                return false;
            }
            else
            {
                var message = $"Unkown type: {result.GetType().FullName}";
                Console.WriteLine(message);
                throw new Exception(message);
            }
        }
        
        private async Task ReleaseMemLock(string shard, string tx, HashSet<string> keys)
        {
            var rid = Guid.NewGuid().ToString();
            var cmd = new ReleaseMemLockCmd
            {
                tx = tx,
                keys = keys,
                Destination = shard,
                Source = this.address,
                ID = rid,
                Size = Consts.AvgMessageSize
            };
            if (this.address == shard)
            {
                await cmd.Execute(this);
            }
            else
            {
                var tcs = new TaskCompletionSource<IMessage>();
                this.replies.Add(rid, tcs);
                await this.network.SendAsync(cmd);
                await tcs.Task;
            }
        }
        
        ////////////////////////////////////////////////////////
        
        private async Task Execute(DataMessage<AppMessages.TransferTx> tx)
        {
            try
            {
                if (tx.Data.Donor == tx.Data.Recipient)
                {
                    var message = $"donor is same as recipient ({tx.Data.Donor})";
                    Console.WriteLine(message);
                    throw new Exception(message);
                }

                var read = await this.core.Change(
                    new HashSet<string> { tx.Data.Donor, tx.Data.Recipient },
                    delegate(Dictionary<string, int> data)
                    {
                        return new Dictionary<string, int>
                        {
                            { tx.Data.Donor, data[tx.Data.Donor] - tx.Data.Amount },
                            { tx.Data.Recipient, data[tx.Data.Recipient] + tx.Data.Amount }
                        };
                    },
                    tx.Source
                );

                var result = new AppMessages.TxOk();
                result.Accounts[tx.Data.Donor] = read[tx.Data.Donor];
                result.Accounts[tx.Data.Recipient] = read[tx.Data.Recipient];

                await this.network.SendAsync(new DataMessage<AppMessages.TxOk>(
                    dest: tx.Source,
                    source: this.address,
                    id: tx.ID,
                    data: result,
                    size: Consts.AvgMessageSize
                ));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw e;
            }
        }
    
        private async Task Execute(DataMessage<AppMessages.ReadTx> tx)
        {
            var read = await this.core.Read(new HashSet<string>(tx.Data.Accounts), tx.Source);

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

        ////////////////////////////////////////////////////////
    }
}