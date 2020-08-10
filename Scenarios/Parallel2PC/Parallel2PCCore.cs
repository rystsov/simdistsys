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

namespace Transactions.Scenarios.Parallel2PC
{
    public class Parallel2PCCore : IMaterializedLocksTM
    {
        //////////////////////////////////////////////////////
        
        protected readonly bool debug = false;
        private readonly long backoffCapUs;
        private readonly int attemptsPerIncrease;
        private readonly bool useBackoff;
        private Node node;
        protected readonly Func<string, string> shardLocator2;
        
        public Parallel2PCCore(Node node, Func<string, string> shardLocator, bool useBackoff, long backoffCapUs, int attemptsPerIncrease)
        {
            this.node = node;
            this.shardLocator2 = shardLocator;
            this.useBackoff = useBackoff;
            this.backoffCapUs = backoffCapUs;
            this.attemptsPerIncrease = attemptsPerIncrease;
        }

        ////////////////////////////////////////////////////////
        
        public async Task<Dictionary<string, int>> Read(HashSet<string> keys, string clientId)
        {
            try
            {
                var isSingleShardRead = keys.GroupBy(shardLocator2).Count() == 1;
                string mytx = Guid.NewGuid().ToString();
            
                if (debug)
                {
                    Console.WriteLine("###########################################################");
                    Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} read {mytx} started");
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
                        return opportunisticRead.data.ToDictionary(x => x.Key, x => x.Value.CurrValue);
                    }
                    
                    var read = opportunisticRead.data;
                    if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} rereading");
                    var reread = (await this.ReadKeys(keys, mytx, clientId)).data;
                    if (read.All(x => read[x.Key].Version == reread[x.Key].Version))
                    {
                        if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} done");
                        return read.ToDictionary(x => x.Key, x => x.Value.CurrValue);
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

                    Dictionary<string, Records.Account3> read;
                    
                    if (isSingleShardRead)
                    {
                        var roe = await TryExecuteKeys(keys, change, mytx, clientId);
                        if (roe.isFinal)
                        {
                            return roe.data.ToDictionary(x => x.Key, x => x.Value.CurrValue);
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
                            if (read[key].LockedByTx == null)
                            {
                                Console.WriteLine($"\t{key} = (ver:{read[key].Version}, curr:{read[key].CurrValue})");
                            }
                            else
                            {
                                Console.WriteLine($"\t{key} = (ver:{read[key].Version}, curr:{read[key].CurrValue}, tx: {read[key].LockedByTx}, next: {read[key].NextValue}, peers: {string.Join(",", read[key].TxKeys)})");
                            }
                        }
                    }

                    var data = change(read.ToDictionary(x => x.Key, x => x.Value.CurrValue));
                    var locks = new List<Record>();
                    foreach (var key in keys)
                    {
                        locks.Add(new Record 
                        {
                            key = key,
                            value = data[key],
                            version = read[key].Version
                        });
                    }
                    var locksByShard = locks.GroupBy(x => shardLocator2(x.key)).ToDictionary(x => x.Key, x => x.ToList());
                    var lockRequests = new Dictionary<string, Task<Dictionary<string, Records.Account3>>>();
                    foreach (var shard in locksByShard.Keys)
                    {
                        lockRequests.Add(shard, LockKeys(shard, mytx, keys, locksByShard[shard]));
                    }
                    var taken = new Dictionary<string, Records.Account3>();
                    var conflicts = new Dictionary<string, Records.Account3>();
                    foreach (var shard in lockRequests.Keys)
                    {
                        var lockShard = await lockRequests[shard];
                        foreach (var key in lockShard.Keys)
                        {
                            var record = lockShard[key];
                            if (record.LockedByTx == mytx)
                            {
                                taken.Add(key, record);
                            }
                            else
                            {
                                conflicts.Add(key, record);
                            }
                        }
                    }
                    
                    if (taken.Count == keys.Count)
                    {
                        var forward = new List<Record>();
                        foreach (var key in taken.Keys)
                        {
                            forward.Add(new Record { key = key, value = taken[key].NextValue, version = taken[key].Version });
                        }
                        _ = RollForward(new Dictionary<string, List<Record>> { { mytx, forward} });
                        if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} done");
                        return data;
                    }
                    else
                    {
                        if (debug)
                        {
                            Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} encounters conflict on locking");
                            foreach (var key in conflicts.Keys)
                            {
                                if (conflicts[key].LockedByTx == null)
                                {
                                    Console.WriteLine($"\t{key} = (ver:{conflicts[key].Version}, curr:{conflicts[key].CurrValue})");
                                }
                                else
                                {
                                    Console.WriteLine($"\t{key} = (ver:{conflicts[key].Version}, curr:{conflicts[key].CurrValue}, tx: {conflicts[key].LockedByTx}, next: {conflicts[key].NextValue}, peers: {string.Join(",", conflicts[key].TxKeys)})");
                                }
                            }
                        }
                        
                        var releases = new List<Record>();
                        foreach (var key in taken.Keys)
                        {
                            releases.Add(new Record { key = key, value = taken[key].CurrValue, version = taken[key].Version });
                        }
                        var releasesByShard = releases.GroupBy(x => shardLocator2(x.key)).ToDictionary(x => x.Key, x => x.ToList());
                        foreach (var shard in releasesByShard.Keys)
                        {
                            _ = CleanKeys(shard, releasesByShard[shard]);
                        }
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

        ////////////////////////////////////////////////////////

        // read(key)

        private class ReadKeysTx : DbNode.IROTx
        {
            private readonly HashSet<string> keys;

            public ReadKeysTx(HashSet<string> keys)
            {
                this.keys = keys;
            }

            public Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var result = new Dictionary<string, Records.Account3>();

                foreach (var key in this.keys)
                {
                    result.Add(key, (Records.Account3)storage.Read(key));
                }

                return Task.FromResult<ICloneable>(new CloneableDictionary<string, Records.Account3>(result));
            }
            
            public object Clone()
            {
                return new ReadKeysTx(this.keys.ToHashSet());
            }
        }

        private async Task<Dictionary<string, Records.Account3>> ReadKeys(string shard, HashSet<string> keys)
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
            return ((CloneableDictionary<string, Records.Account3>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
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
                var result = new Dictionary<string, Records.Account3>();

                foreach (var key in this.keys)
                {
                    result.Add(key, (Records.Account3)storage.Read(key));
                }

                if (result.Values.All(x => x.LockedByTx == null))
                {
                    var changed = change(result.ToDictionary(x => x.Key, x => x.Value.CurrValue));
                    foreach (var key in result.Keys)
                    {
                        var account = result[key];
                        account.CurrValue = changed[key];
                        account.Version+=1;
                        storage.Write(key, account);
                    }
                }

                return new CloneableDictionary<string, Records.Account3>(result);
            }
            
            public object Clone()
            {
                return new TryExecuteKeysTx(this.keys.ToHashSet(), change);
            }
        }

        private async Task<Dictionary<string, Records.Account3>> TryExecuteKeys(string shard, HashSet<string> keys, Func<Dictionary<string, int>, Dictionary<string, int>> change)
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
            return ((CloneableDictionary<string, Records.Account3>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }
        
        // touch(key)

        private class TouchKeysTx : DbNode.IROTx, DbNode.IRWTx
        {
            private readonly HashSet<string> keys;

            public TouchKeysTx(HashSet<string> keys)
            {
                this.keys = keys;
            }

            public async Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var result = new Dictionary<string, Records.Account3>();

                foreach (var key in this.keys)
                {
                    var account = (Records.Account3)storage.Read(key);

                    if (account.LockedByTx != null)
                    {
                        result.Add(key, account);
                    }
                }

                if (result.Count == this.keys.Count)
                {
                    return new CloneableDictionary<string, Records.Account3>(result);
                }
                else
                {
                    return await storage.Execute(this);
                }
            }

            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var result = new Dictionary<string, Records.Account3>();

                foreach (var key in this.keys)
                {
                    var account = (Records.Account3)storage.Read(key);

                    if (account.LockedByTx == null)
                    {
                        account.Version++;
                        storage.Write(key, account);
                    }

                    result.Add(key, account);
                }

                return new CloneableDictionary<string, Records.Account3>(result);
            }
            
            public object Clone()
            {
                return new TouchKeysTx(this.keys.ToHashSet());
            }
        }

        private async Task<Dictionary<string, Records.Account3>> TouchKeys(string shard, HashSet<string> keys)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new TouchKeysTx(keys),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account3>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }
        
        // lock(key, ver, value)

        private class Record : ICloneable
        {
            public string key;
            public ulong version;
            public int value;

            public object Clone()
            {
                return new Record
                {
                    key = this.key,
                    version = this.version,
                    value = this.value
                };
            }
        }

        private class LockKeysTx : DbNode.IRWTx
        {
            private readonly List<Record> records;
            private readonly HashSet<string> keys;
            private readonly string tx;

            public LockKeysTx(string tx, HashSet<string> keys, List<Record> records)
            {
                this.tx = tx;
                this.records = records;
                this.keys = keys;
            }

            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var isClean = true;

                var result = new Dictionary<string, Records.Account3>();

                foreach (var record in this.records)
                {
                    var account = (Records.Account3)storage.Read(record.key);
                    result.Add(record.key, account);

                    if (account.Version != record.version)
                    {
                        isClean = false;
                    }
                }

                if (isClean)
                {
                    result.Clear();
                    foreach (var record in this.records)
                    {
                        var account = (Records.Account3)storage.Read(record.key);
                        account.LockedByTx = this.tx;
                        account.TxKeys = this.keys.ToList();
                        account.NextValue = record.value;
                        account.Version++;
                        storage.Write(record.key, account);
                        result.Add(record.key, account);
                    }
                }
                
                return new CloneableDictionary<string, Records.Account3>(result);
            }
            
            public object Clone()
            {
                return new LockKeysTx(this.tx, this.keys.ToHashSet(), this.records.Select(x => (Record)x.Clone()).ToList());
            }
        }
        
        private async Task<Dictionary<string, Records.Account3>> LockKeys(string shard, string tx, HashSet<string> keys, List<Record> records)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IRWTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new LockKeysTx(tx, keys, records),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account3>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }
        
        // clean(key, ver, value)

        private class CleanKeysTx : DbNode.IROTx, DbNode.IRWTx
        {
            private readonly List<Record> records;

            public CleanKeysTx(List<Record> records)
            {
                this.records = records;
            }

            public async Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var result = new Dictionary<string, Records.Account3>();

                foreach (var record in this.records)
                {
                    var account = (Records.Account3)storage.Read(record.key);

                    if (account.Version != record.version)
                    {
                        result.Add(record.key, account);
                    }
                }

                if (result.Count == this.records.Count)
                {
                    return new CloneableDictionary<string, Records.Account3>(result);
                }
                else
                {
                    return await storage.Execute(this);
                }
            }

            public ICloneable Execute(DbNode.IRWStorage storage)
            {
                var result = new Dictionary<string, Records.Account3>();

                foreach (var record in this.records)
                {
                    var account = (Records.Account3)storage.Read(record.key);

                    if (account.Version == record.version)
                    {
                        account.CurrValue = record.value;
                        account.LockedByTx = null;
                        account.Version++;
                        storage.Write(record.key, account);
                    }
                    result.Add(record.key, account);
                }

                return new CloneableDictionary<string, Records.Account3>(result);
            }
            
            public object Clone()
            {
                return new CleanKeysTx(this.records.Select(x => (Record)x.Clone()).ToList());
            }
        }

        private async Task<Dictionary<string, Records.Account3>> CleanKeys(string shard, List<Record> records)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new CleanKeysTx(records),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Account3>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }
        
        // write-tx(tx)

        private class CommitTxTx : DbNode.IRWTx, DbNode.IROTx
        {
            private readonly string tx;

            public CommitTxTx(string tx)
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

                var tx = new Records.Tx();
                storage.Write(this.tx, tx);
                return tx;
            }

            public object Clone()
            {
                return new CommitTxTx(this.tx);
            }
        }

        private async Task<Records.Tx> CommitTx(string tx)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: this.shardLocator2(tx),
                source: this.node.address,
                id: rid,
                data: new CommitTxTx(tx),
                size: Consts.AvgMessageSize
            ));
            return (Records.Tx)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result;
        }
        
        // read-tx(tx)

        private class ReadTxTx : DbNode.IROTx
        {
            private readonly HashSet<string> txes;

            public ReadTxTx(HashSet<string> txes)
            {
                this.txes = txes;
            }

            public Task<ICloneable> Execute(DbNode.IROStorage storage)
            {
                var result = new Dictionary<string, Records.Tx>();

                foreach (var tx in this.txes)
                {
                    result.Add(tx, storage.Has(tx) ? (Records.Tx)storage.Read(tx) : null);
                }
                
                return Task.FromResult<ICloneable>(new CloneableDictionary<string, Records.Tx>(result));
            }

            public object Clone()
            {
                return new ReadTxTx(this.txes.ToHashSet());
            }
        }
        
        private async Task<Dictionary<string, Records.Tx>> ReadTx(string shard, HashSet<string> txes)
        {
            var rid = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<IMessage>();
            this.node.replies.Add(rid, tcs);
            await this.node.network.SendAsync(new DataMessage<DbNode.IROTx>(
                dest: shard,
                source: this.node.address,
                id: rid,
                data: new ReadTxTx(txes),
                size: Consts.AvgMessageSize
            ));
            return ((CloneableDictionary<string, Records.Tx>)((DataMessage<DbNode.TxResult>)(await tcs.Task)).Data.Result).data;
        }
        
        //////////////////////////////////////////////////////////////////////////////////////

        private class OpportunisticRead
        {
            public bool isFinal;
            public Dictionary<string, Records.Account3> data;
        }
        
        private async Task<OpportunisticRead> TryExecuteKeys(HashSet<string> keys, Func<Dictionary<string, int>, Dictionary<string, int>> change, string mytx, string clientId)
        {
            try
            {
                if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} reading/executing keys: {string.Join(", ", keys)}");
                
                var read = await TryExecuteKeys(shardLocator2(keys.First()), keys, change);
                
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
            try
            {
                if (debug) Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} reading keys: {string.Join(", ", keys)}");
                
                var keysByShard = keys.GroupBy(this.shardLocator2).ToDictionary(x => x.Key, x => x.ToHashSet());
                var readRequests = new Dictionary<string, Task<Dictionary<string, Records.Account3>>>();
                foreach (var shard in keysByShard.Keys)
                {
                    readRequests.Add(shard, ReadKeys(shard, keysByShard[shard]));
                }

                var read = new Dictionary<string, Records.Account3>();
                
                foreach (var shard in readRequests.Keys)
                {
                    var readShard = await readRequests[shard];
                    foreach (var key in readShard.Keys)
                    {
                        read.Add(key, readShard[key]);
                    }
                }

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

        private async Task<Dictionary<string, Records.Account3>> FixRead(Dictionary<string, Records.Account3> read, string mytx, string clientId)
        {
            // tx -> [key]
            var dirty = new Dictionary<string, List<string>>();

            foreach (var key in read.Keys)
            {
                if (read[key].LockedByTx != null)
                {
                    if (!dirty.ContainsKey(read[key].LockedByTx))
                    {
                        dirty.Add(read[key].LockedByTx, read[key].TxKeys.ToList());
                    }
                }
            }

            if (dirty.Count == 0)
            {
                return read;
            }

            if (debug)
            {
                Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} read keys");
                foreach (var key in read.Keys)
                {
                    if (read[key].LockedByTx == null)
                    {
                        Console.WriteLine($"\t{key} = (ver:{read[key].Version}, curr:{read[key].CurrValue})");
                    }
                    else
                    {
                        Console.WriteLine($"\t{key} = (ver:{read[key].Version}, curr:{read[key].CurrValue}, tx: {read[key].LockedByTx}, next: {read[key].NextValue}, peers: {string.Join(",", read[key].TxKeys)})");
                    }
                }
            }

            var touchKeys = new HashSet<string>();
            var touched = new Dictionary<string, Records.Account3>();
            foreach (var tx in dirty.Keys)
            {
                foreach (var key in dirty[tx])
                {
                    if (read.ContainsKey(key) && read[key].LockedByTx != null)
                    {
                        if (!touched.ContainsKey(key))
                        {
                            touched.Add(key, read[key]);
                        }
                    }
                    else
                    {
                        touchKeys.Add(key);
                    }
                }
            }
            var touchKeysByShard = touchKeys.GroupBy(this.shardLocator2).ToDictionary(x => x.Key, x => x.ToHashSet());
            var touchRequests = new Dictionary<string, Task<Dictionary<string, Records.Account3>>>();
            foreach (var shard in touchKeysByShard.Keys)
            {
                touchRequests.Add(shard, TouchKeys(shard, touchKeysByShard[shard]));
            }
            foreach (var shard in touchRequests.Keys)
            {
                var touchedShard = await touchRequests[shard];
                foreach (var key in touchedShard.Keys)
                {
                    if (read.ContainsKey(key) && touchedShard[key].LockedByTx == null)
                    {
                        read[key] = touchedShard[key];
                    }
                    
                    touched.Add(key, touchedShard[key]);
                }
            }

            if (debug)
            {
                Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} read tx peers");
                foreach (var key in touched.Keys)
                {
                    if (touched[key].LockedByTx == null)
                    {
                        Console.WriteLine($"\t{key} = (ver:{touched[key].Version}, curr:{touched[key].CurrValue})");
                    }
                    else
                    {
                        Console.WriteLine($"\t{key} = (ver:{touched[key].Version}, curr:{touched[key].CurrValue}, tx: {touched[key].LockedByTx}, next: {touched[key].NextValue}, peers: {string.Join(",", touched[key].TxKeys)})");
                    }
                }
            }

            var isTxComitted = new Dictionary<string, bool>();

            var txes = new HashSet<string>();
            foreach (var tx in dirty.Keys)
            {
                if (dirty[tx].All(x => touched[x].LockedByTx == tx))
                {
                    isTxComitted.Add(tx, true);
                }
                else
                {
                    txes.Add(tx);
                }
            }
            var txesByShard = txes.GroupBy(this.shardLocator2).ToDictionary(x => x.Key, x => x.ToHashSet());
            var txesRequests = new Dictionary<string, Task<Dictionary<string, Records.Tx>>>();
            foreach (var shard in txesByShard.Keys)
            {
                txesRequests.Add(shard, ReadTx(shard, txesByShard[shard]));
            }
            foreach (var shard in txesRequests.Keys)
            {
                var txShard = await txesRequests[shard];
                foreach (var tx in txShard.Keys)
                {
                    isTxComitted.Add(tx, txShard[tx] != null);
                }
            }

            if (debug)
            {
                Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} deduce:");
                foreach (var tx in isTxComitted.Keys)
                {
                    if (isTxComitted[tx])
                    {
                        Console.WriteLine($"\t{tx} - comitted");
                    }
                    else
                    {
                        Console.WriteLine($"\t{tx} - aborted");
                    }
                }
            }

            var rollBackward = new List<Record>();
            var rollForward = new Dictionary<string, List<Record>>();

            foreach (var key in read.Keys)
            {
                if (touched.ContainsKey(key))
                {
                    touched.Remove(key);
                }
                if (read[key].LockedByTx != null)
                {
                    if (isTxComitted[read[key].LockedByTx])
                    {
                        if (!rollForward.ContainsKey(read[key].LockedByTx))
                        {
                            rollForward.Add(read[key].LockedByTx, new List<Record>());
                        }
                        rollForward[read[key].LockedByTx].Add(new Record { key = key, version = read[key].Version, value = read[key].NextValue });
                        read[key].Version++;
                        read[key].CurrValue = read[key].NextValue;
                        read[key].LockedByTx = null;
                        read[key].TxKeys = null;
                    }
                    else
                    {
                        rollBackward.Add(new Record { key = key, version = read[key].Version, value = read[key].CurrValue });
                        read[key].Version++;
                        read[key].LockedByTx = null;
                        read[key].TxKeys = null;
                    }
                }
            }
            if (debug)
            {
                Console.WriteLine($"[{clientId}] {this.node.clock.Now.value} {mytx} rolling:");
                Console.WriteLine($"\tbackward: {string.Join(",", rollBackward.Select(x => x.key))}");
                Console.WriteLine($"\tforward: {string.Join(",", rollForward.SelectMany(x => x.Value.Select(x => x.key)))}");
            }

            foreach (var tx in dirty.Keys)
            {
                foreach (var key in dirty[tx])
                {
                    if (touched.ContainsKey(key))
                    {
                        if (touched[key].LockedByTx == tx)
                        {
                            if (isTxComitted[tx])
                            {
                                if (!rollForward.ContainsKey(tx))
                                {
                                    rollForward.Add(tx, new List<Record>());
                                }
                                rollForward[tx].Add(new Record { key = key, version = touched[key].Version, value = touched[key].NextValue });
                            }
                            else
                            {
                                rollBackward.Add(new Record { key = key, version = touched[key].Version, value = touched[key].CurrValue });
                            }
                        }
                        touched.Remove(key);
                    }
                }
            }

            var rollBackwardByShard = rollBackward.GroupBy(x => shardLocator2(x.key)).ToDictionary(x => x.Key, x => x.ToList());
            foreach (var shard in rollBackwardByShard.Keys)
            {
                _ = this.CleanKeys(shard, rollBackwardByShard[shard]);
            }

            _ = RollForward(rollForward);

            return read;
        }

        private async Task RollForward(Dictionary<string, List<Record>> rollForwardByTx)
        {
            try
            {
                var commitRequests = new List<Task<Records.Tx>>();
                var rollForward = new List<Record>();
                foreach (var tx in rollForwardByTx.Keys)
                {
                    commitRequests.Add(CommitTx(tx));
                    rollForward.AddRange(rollForwardByTx[tx]);
                }
                
                foreach (var commitRequest in commitRequests)
                {
                    await commitRequest;
                }

                var rollForwardByShard = rollForward.GroupBy(x => shardLocator2(x.key)).ToDictionary(x => x.Key, x => x.ToList());
                foreach (var shard in rollForwardByShard.Keys)
                {
                    _ = CleanKeys(shard, rollForwardByShard[shard]);
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                throw e;
            }
        }
    }
}