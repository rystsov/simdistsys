using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Mem;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.OCC2PC
{
    public class OCC2PCCore : IMaterializedLocksTM
    {
        private class OpportunisticRead
        {
            public bool isFinal;
            public Dictionary<string, Records.Account> data;
        }
        
        private class CommitTxTx : DbNode.IRWTx, DbNode.IROTx
        {
            private readonly string tx;
            private readonly Dictionary<string, int> values;

            public CommitTxTx(string tx, Dictionary<string, int> values)
            {
                this.tx = tx;
                this.values = values;
            }

            public async Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                if (storage.Has(this.tx))
                {
                    return (Records.Tx)storage.Read(this.tx);
                }
                else
                {
                    return await storage.Execute(this);
                }
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
        
        private class AbortTxTx : DbNode.IRWTx, DbNode.IROTx
        {
            private readonly string tx;

            public AbortTxTx(string tx)
            {
                this.tx = tx;
            }

            public async Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                if (storage.Has(this.tx))
                {
                    return (Records.Tx)storage.Read(this.tx);
                }
                else
                {
                    return await storage.Execute(this);
                }
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

        private class BlockKeysTx : DbNode.IRWTx, DbNode.IROTx
        {
            private readonly Dictionary<string, ulong> keyVersion;
            private readonly string tx;
            private readonly HashSet<string> allKeys;

            public BlockKeysTx(Dictionary<string, ulong> keyVersion, string tx, HashSet<string> allKeys)
            {
                this.keyVersion = keyVersion;
                this.tx = tx;
                this.allKeys = allKeys;
            }

            public async Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var isClean = true;
                var result = new Dictionary<string, Records.Account>();
                foreach (var key in this.keyVersion.Keys)
                {
                    var account = (Records.Account)storage.Read(key);
                    isClean &= account.Version == this.keyVersion[key];
                    result.Add(key, account);
                }

                if (isClean)
                {
                    return await storage.Execute(this);
                }
                else
                {
                    return new CloneableDictionary<string, Records.Account>(result);
                }
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var isClean = true;
                var result = new Dictionary<string, Records.Account>();

                foreach (var key in this.keyVersion.Keys)
                {
                    var account = (Records.Account)storage.Read(key);
                    isClean &= account.Version == this.keyVersion[key];
                    result.Add(key, account);
                }

                if (isClean)
                {
                    foreach (var key in result.Keys)
                    {
                        var account = result[key];
                        account.LockedByTx = this.tx;
                        account.TxKeys = this.allKeys.ToList();
                        account.Version+=1;
                        storage.Write(key, account);
                    }
                }
                
                return new CloneableDictionary<string, Records.Account>(result);
            }

            public object Clone()
            {
                return new BlockKeysTx(this.keyVersion.ToDictionary(x => x.Key, x => x.Value), this.tx, this.allKeys.ToHashSet());
            }
        }

        private class ReadKeysTx : DbNode.IROTx
        {
            private readonly HashSet<string> keys;

            public ReadKeysTx(HashSet<string> keys)
            {
                this.keys = keys;
            }
            
            public Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var result = new Dictionary<string, Records.Account>();

                foreach (var key in keys)
                {
                    var account = (Records.Account)storage.Read(key);
                    result.Add(key, account);
                }
                
                return Task.FromResult<ICloneable>(new CloneableDictionary<string, Records.Account>(result));
            }

            public object Clone()
            {
                return new ReadKeysTx(this.keys.ToHashSet());
            }
        }

        private class TryExecuteKeysTx : DbNode.IRWTx
        {
            private readonly HashSet<string> keys;
            private readonly Func<Dictionary<string, int>, Dictionary<string, int>> change;

            public TryExecuteKeysTx(HashSet<string> keys, Func<Dictionary<string, int>, Dictionary<string, int>> change)
            {
                this.keys = keys;
                this.change = change;
            }

            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var result = new Dictionary<string, Records.Account>();

                foreach (var key in this.keys)
                {
                    result.Add(key, (Records.Account)storage.Read(key));
                }

                if (result.Values.All(x => x.LockedByTx == null))
                {
                    var changed = change(result.ToDictionary(x => x.Key, x => x.Value.Value));
                    foreach (var key in result.Keys)
                    {
                        var account = result[key];
                        account.Value = changed[key];
                        account.Version+=1;
                        storage.Write(key, account);
                    }
                }

                return new CloneableDictionary<string, Records.Account>(result);
            }
            
            public object Clone()
            {
                return new TryExecuteKeysTx(this.keys.ToHashSet(), change);
            }
        }

        private class ReleaseKeysTx : DbNode.IRWTx, DbNode.IROTx
        {
            private readonly Dictionary<string, ReleaseOp> releases;

            public ReleaseKeysTx(Dictionary<string, ReleaseOp> releases)
            {
                this.releases = releases;
            }

            public async Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var result = new Dictionary<string, Records.Account>();
                var hasLocks = false;

                foreach (var key in this.releases.Keys)
                {
                    var account = (Records.Account)storage.Read(key);
                    hasLocks |= account.Version == this.releases[key].version;
                    result.Add(key, account);
                }

                if (hasLocks)
                {
                    return await storage.Execute(this);
                }
                else
                {
                    return new CloneableDictionary<string, Records.Account>(result);
                }
            }
            
            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var result = new Dictionary<string, Records.Account>();

                foreach (var key in this.releases.Keys)
                {
                    var account = (Records.Account)storage.Read(key);
                    if (account.Version == this.releases[key].version)
                    {
                        account.LockedByTx = null;
                        account.TxKeys = null;
                        account.Value = this.releases[key].value;
                        account.Version++;
                        storage.Write(key, account);
                    }
                    result.Add(key, account);
                }

                return new CloneableDictionary<string, Records.Account>(result);
            }

            public object Clone()
            {
                return new ReleaseKeysTx(this.releases.ToDictionary(x => x.Key, x => x.Value));
            }
        }

        private class ReleaseOp
        {
            public readonly ulong version;
            public readonly int value;

            public ReleaseOp(ulong version, int value)
            {
                this.version = version;
                this.value = value;
            }
        }

        
        
        //////////////////////////////////////////////////////
        
        protected readonly bool debug = false;
        private readonly long backoffCapUs;
        private readonly int attemptsPerIncrease;
        private readonly bool useBackoff;
        private Node node;
        protected readonly Func<string, string> shardLocator2;
        
        public OCC2PCCore(Node node, Func<string, string> shardLocator, bool useBackoff, long backoffCapUs, int attemptsPerIncrease)
        {
            this.node = node;
            this.shardLocator2 = shardLocator;
            this.useBackoff = useBackoff;
            this.backoffCapUs = backoffCapUs;
            this.attemptsPerIncrease = attemptsPerIncrease;
        }

        public async Task<Dictionary<string, int>> Read(HashSet<string> keys, string clientId)
        {
            try
            {
                var isSingleShardRead = keys.GroupBy(shardLocator2).Count() == 1;
                string mytx = Guid.NewGuid().ToString();
            
                var attempts = 0;
                var backoff = 1;
                while (true)
                {
                    if (useBackoff && attempts > 1)
                    {
                        if (backoff < this.backoffCapUs)
                        {
                            if (attempts % attemptsPerIncrease == 0)
                            {
                                backoff *= 2;
                            }
                        }
                        await this.node.clock.Delay(new Microsecond((ulong)this.node.random.Next(backoff)));
                    }
                    attempts++;

                    var opportunisticRead = await this.ReadKeys(keys, mytx, clientId);
                    if (isSingleShardRead && opportunisticRead.isFinal)
                    {
                        return opportunisticRead.data.ToDictionary(x => x.Key, x => x.Value.Value);
                    }
                    
                    var read = opportunisticRead.data;
                    var reread = (await this.ReadKeys(keys, mytx, clientId)).data;
                    if (read.All(x => read[x.Key].Version == reread[x.Key].Version))
                    {
                        return read.ToDictionary(x => x.Key, x => x.Value.Value);
                    }
                    else
                    {
                        if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} retry");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw e;
            }
        }

        public async Task<Dictionary<string, int>> Change(HashSet<string> keys, Func<Dictionary<string, int>, Dictionary<string, int>> change, string clientId)
        {
            try
            {
                var isSingleShardRead = keys.GroupBy(shardLocator2).Count() == 1;
                string mytx = Guid.NewGuid().ToString();
            
                if (debug)
                {
                    Console.WriteLine("###########################################################");
                    Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} change {mytx} started");
                    // Console.WriteLine("\treading:");
                    // foreach (var key in keys)
                    // {
                    //     Console.WriteLine($"\t\t{key}");
                    // }
                    // Console.WriteLine();
                }

                var attempts = 0;
                var backoff = 1;
                while (true)
                {
                    if (useBackoff & attempts > 1)
                    {
                        if (backoff < this.backoffCapUs)
                        {
                            if (attempts % attemptsPerIncrease == 0)
                            {
                                backoff *= 2;
                            }
                        }
                        await this.node.clock.Delay(new Microsecond((ulong)this.node.random.Next(backoff)));
                    }
                    attempts++;

                    Dictionary<string, Records.Account> read;
                    
                    if (isSingleShardRead)
                    {
                        var roe = await TryExecuteKeys(keys, change, mytx, clientId);
                        if (roe.isFinal)
                        {
                            return roe.data.ToDictionary(x => x.Key, x => x.Value.Value);
                        }
                        else
                        {
                            read = roe.data;
                        }
                    }
                    else
                    {
                        read = (await ReadKeys(keys, mytx, clientId)).data;
                    }

                    if (debug)
                    {
                        Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} read keys");
                        foreach (var key in read.Keys)
                        {
                            Console.WriteLine($"\t{key} = (ver:{read[key].Version}, curr:{read[key].Value})");
                        }
                    }

                    var data = change(read.ToDictionary(x => x.Key, x => x.Value.Value));
                    
                    var blocked = await this.BlockKeys(read.ToDictionary(x => x.Key, x => x.Value.Version), mytx);


                    Dictionary<string, ReleaseOp> releases;
                    var hasExecuted = false;
                    
                    if (blocked.All(x => x.Value.LockedByTx == mytx))
                    {
                        if (debug)
                        {
                            Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} blocked");
                            foreach (var key in blocked.Keys)
                            {
                                Console.WriteLine($"\t{key} -> ver:{blocked[key].Version} val:{blocked[key].Value}");
                            }
                        }
                        var tx = await this.CommitTx(mytx, data);
                        if (debug)
                        {
                            Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} commits");
                            foreach (var key in data.Keys)
                            {
                                Console.WriteLine($"\t{key} = {data[key]}");
                            }
                        }
                        if (tx.State == Records.Tx.TxState.Committed)
                        {
                            releases = blocked.ToDictionary(x => x.Key, x => new ReleaseOp(x.Value.Version, data[x.Key]));
                            hasExecuted = true;
                        }
                        else
                        {
                            if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} was aborted before it's committed");
                            releases = blocked.ToDictionary(x => x.Key, x => new ReleaseOp(x.Value.Version, read[x.Key].Value));
                        }
                    }
                    else
                    {
                        if (debug)
                        {
                            Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} encounters conflict on locking");
                            foreach (var key in blocked.Keys)
                            {
                                Console.WriteLine($"\t{key} conflicted on ver:{blocked[key].Version} val:{blocked[key].Value}");
                            }
                        }
                        releases = blocked.Where(x => x.Value.LockedByTx == mytx).ToDictionary(x => x.Key, x => new ReleaseOp(x.Value.Version, read[x.Key].Value));
                    }

                    var releasesByShard = releases.GroupBy(x => shardLocator2(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));

                    foreach (var shard in releasesByShard.Keys)
                    {
                        _ = this.ReleaseKeysOnShard(shard, releasesByShard[shard]);
                    }

                    if (hasExecuted)
                    {
                        if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} done");
                        return data;
                    }
                    else
                    {
                        var newtx = Guid.NewGuid().ToString();
                        if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} -> {newtx} retry");
                        mytx = newtx;
                    }
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
        
        //////////////////////////////////////////////////////

        private async Task<OpportunisticRead> TryExecuteKeys(HashSet<string> keys, Func<Dictionary<string, int>, Dictionary<string, int>> change, string mytx, string clientId)
        {
            try
            {
                if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} reading/executing keys: {string.Join(", ", keys)}");
                
                var read = await this.TryExecuteKeys(shardLocator2(keys.First()), keys, change);
                
                if (read.Values.All(x => x.LockedByTx == null))
                {
                    return new OpportunisticRead { isFinal = true, data = read };
                }
                else
                {
                    return new OpportunisticRead { isFinal = false, data = await FixRead(read, mytx, clientId) };
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                throw e;
            }
        }

        private async Task<OpportunisticRead> ReadKeys(HashSet<string> keys, string mytx, string clientId)
        {
            if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} reading keys: {string.Join(", ", keys)}");
            
            var reading = new Dictionary<string, Task<Dictionary<string,Records.Account>>>();  
                
            var keysByShards = keys.GroupBy(x => shardLocator2(x)).ToDictionary(x => x.Key, x => x.ToHashSet());
                
            foreach (var shard in keysByShards.Keys)
            {
                reading.Add(shard, this.ReadKeysFromShard(shard, keysByShards[shard]));
            }

            // flat the responce

            var read = new Dictionary<string, Records.Account>();

            foreach (var shard in reading.Keys)
            {
                var accounts = await reading[shard];

                foreach (var key in accounts.Keys)
                {
                    var account = accounts[key];
                    read.Add(key, account);
                }
            }

            if (read.All(x => x.Value.LockedByTx == null))
            {
                return new OpportunisticRead
                {
                    data = read,
                    isFinal = true
                };
            }
            else
            {
                return new OpportunisticRead
                {
                    data = await FixRead(read, mytx, clientId),
                    isFinal = false
                };
            }
        }

        private async Task<Dictionary<string, Records.Account>> FixRead(Dictionary<string, Records.Account> read, string mytx, string clientId)
        {
            if (debug)
            {
                Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} read keys");
                foreach (var key in read.Keys)
                {
                    if (read[key].LockedByTx == null)
                    {
                        Console.WriteLine($"\t{key} = (ver:{read[key].Version}, curr:{read[key].Value})");
                    }
                    else
                    {
                        Console.WriteLine($"\t{key} = (ver:{read[key].Version}, curr:{read[key].Value}, tx: {read[key].LockedByTx}, peers: {string.Join(",", read[key].TxKeys)})");
                    }
                }
            }

            var conflicts = new Dictionary<string, HashSet<string>>();

            foreach (var key in read.Keys)
            {
                var account = read[key];
                if (account.LockedByTx != null)
                {
                    if (!conflicts.ContainsKey(account.LockedByTx))
                    {
                        conflicts.Add(account.LockedByTx, new HashSet<string>());
                    }
                    conflicts[account.LockedByTx].Add(key);
                }
            }

            var aborting = new Dictionary<string, Task<Records.Tx>>();
            foreach (var tx in conflicts.Keys)
            {
                if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} aborting {tx}");
                aborting.Add(tx, AbortTx(tx));
            }

            var txStatus = new Dictionary<string, Records.Tx>();

            foreach (var tx in aborting.Keys)
            {
                txStatus.Add(tx, await aborting[tx]);
                if (txStatus[tx].State == Records.Tx.TxState.Aborted)
                {
                    if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} aborted {tx}");
                }
                else
                {
                    if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} can't abort {tx}, already comitted");
                }
            }

            var releases = new Dictionary<string, ReleaseOp>();

            if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} rolling");
            foreach (var key in read.Keys)
            {
                if (read[key].LockedByTx != null)
                {
                    if (txStatus[read[key].LockedByTx].State == Records.Tx.TxState.Committed)
                    {
                        if (debug) Console.WriteLine($"\t{key} to ver:{read[key].Version+1} val:{txStatus[read[key].LockedByTx].Values[key]}");
                        releases.Add(key, new ReleaseOp(read[key].Version, txStatus[read[key].LockedByTx].Values[key]));
                        read[key].Value = txStatus[read[key].LockedByTx].Values[key];
                        read[key].LockedByTx = null;
                        read[key].TxKeys = null;
                        read[key].Version+=1;
                    }
                    else
                    {
                        if (debug) Console.WriteLine($"\t{key} to ver:{read[key].Version+1} val:{read[key].Value}");
                        releases.Add(key, new ReleaseOp(read[key].Version, read[key].Value));
                        read[key].LockedByTx = null;
                        read[key].TxKeys = null;
                        read[key].Version+=1;
                    }
                }
            }

            var releasesByShard = releases.GroupBy(x => shardLocator2(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));

            foreach (var shard in releasesByShard.Keys)
            {
                _ = this.ReleaseKeysOnShard(shard, releasesByShard[shard]);
            }

            return read;
        }

        private async Task<Dictionary<string, Records.Account>> ReadKeysFromShard(string shard, HashSet<string> keys)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new ReadKeysTx(keys),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }

        private async Task ReleaseKeysOnShard(string shard, Dictionary<string, ReleaseOp> releases)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new ReleaseKeysTx(releases),
                size: Consts.AvgMessageSize
            ));
            await tcs.Task;
        }

        private async Task<Dictionary<string, Records.Account>> BlockKeys(Dictionary<string, ulong> keyVersion, string tx)
        {
            var keyVersionByShard = keyVersion.GroupBy(x => shardLocator2(x.Key)).ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key, y => y.Value));
            var blocking = new Dictionary<string, Task<Dictionary<string, Records.Account>>>();
            foreach(var shard in keyVersionByShard.Keys)
            {
                blocking.Add(shard, this.BlockKeysOnShard(shard, keyVersionByShard[shard], keyVersion.Keys.ToHashSet(), tx));
            }

            var blocked = new Dictionary<string, Records.Account>();
            foreach(var shard in blocking.Keys)
            {
                var blockedShard = await blocking[shard];
                foreach (var key in blockedShard.Keys)
                {
                    blocked.Add(key, blockedShard[key]);
                }
            }
            
            return blocked;
        }

        private async Task<Dictionary<string, Records.Account>> BlockKeysOnShard(string shard, Dictionary<string, ulong> keyVersion, HashSet<string> allKeys, string tx)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new BlockKeysTx(keyVersion, tx, allKeys),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }

        private async Task<Dictionary<string, Records.Account>> TryExecuteKeys(string shard, HashSet<string> keys, Func<Dictionary<string, int>, Dictionary<string, int>> change)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new TryExecuteKeysTx(keys, change),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }

        private async Task<Records.Tx> AbortTx(string tx)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: this.shardLocator2(tx),
                source: this.node.address,
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
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: this.shardLocator2(tx),
                source: this.node.address,
                id: rid,
                data: new CommitTxTx(tx, values),
                size: Consts.AvgMessageSize
            ));
            return (Records.Tx)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result;
        }
    }
}