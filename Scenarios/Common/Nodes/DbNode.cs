using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.Common.Nodes
{
    public class DbNode : Node, IDbNode
    {
        public interface IROStorage
        {
            ICloneable Read(string key);
            bool Has(string key);
            Task<ICloneable> Execute(IRWTx tx);
        }

        public interface IRWStorage : IROStorage
        {
            void Delete(string key);
            void Write(string key, ICloneable value);
        }

        public interface IROTx : ICloneable
        {
            Task<ICloneable> Execute(IROStorage storage);
        }

        public interface IRWTx : ICloneable
        {
            ICloneable Execute(IRWStorage storage);
        }

        public abstract class Marker<T> : ICloneable where T : Marker<T>, new()
        {
            public object Clone()
            {
                return new T();
            }
        }

        public class TxResult : ICloneable
        {
            public ICloneable Result { get; private set; }
            
            public TxResult(ICloneable result)
            {
                this.Result = result;
            }
            
            public object Clone()
            {
                return new TxResult(this.Result == null ? null : (ICloneable)this.Result.Clone());
            }
        }

        private class Storage : IRWStorage
        {
            private readonly Dictionary<string, ICloneable> storage;
            private readonly SSD ssd;
            private readonly uint txSize;

            public Storage(Dictionary<string, ICloneable> storage, SSD ssd, uint txSize)
            {
                this.storage = storage;
                this.ssd = ssd;
                this.txSize = txSize;
            }

            public bool Has(string key)
            {
                return this.storage.ContainsKey(key);
            }

            public async Task<ICloneable> Execute(IRWTx tx)
            {
                await this.ssd.WriteAsync(this.txSize);
                return tx.Execute(this);
            }
            
            public ICloneable Read(string key)
            {
                return (ICloneable)this.storage[key].Clone();
            }

            public void Write(string key, ICloneable value)
            {
                this.storage[key] = (ICloneable)value.Clone();
            }

            public void Delete(string key)
            {
                this.storage.Remove(key);
            }
        }

        protected readonly Dictionary<string, ICloneable> storage = new Dictionary<string, ICloneable>();

        protected readonly SSD ssd;

        public DbNode(IEndpoint network, IClock clock, IRandom random, string address, SSDSpec io) : base(network, clock, random, address)
        {
            this.ssd = new SSD(clock, io);
        }
        
        public async Task Run()
        {
            while (true)
            {
                var message = await this.network.ReceiveAsync();

                this.Process(message);
            }
        }

        protected virtual void Process(IMessage message)
        {
            if (message is DataMessage<IROTx> rotx)
            {
                _ = this.ProcessRO(rotx);
            }
            else if (message is DataMessage<IRWTx> rwtx)
            {
                _ = this.ProcessRW(rwtx);
            }
            else
            {
                Console.WriteLine($"{nameof(DbNode)}: Unexpected message: {message.GetType().FullName}");
                throw new Exception();
            }
        }
    
        protected async Task<ICloneable> ExecuteRO(IROTx tx, uint txSize)
        {
            var storage = new Storage(this.storage, this.ssd, txSize);
            return await tx.Execute(storage);
        }

        protected async Task<ICloneable> ExecuteRW(IRWTx tx, uint txSize)
        {
            var storage = new Storage(this.storage, this.ssd, txSize);
            return await storage.Execute(tx);
        }

        protected async Task ProcessRO(DataMessage<IROTx> tx)
        {
            await this.network.SendAsync(new DataMessage<TxResult>(
                dest: tx.Source,
                source: this.address,
                id: tx.ID,
                data: new TxResult(await ExecuteRO(tx.Data, tx.Size)),
                size: Consts.AvgMessageSize
            ));
        }

        protected async Task ProcessRW(DataMessage<IRWTx> tx)
        {
            await this.network.SendAsync(new DataMessage<TxResult>(
                dest: tx.Source,
                source: this.address,
                id: tx.ID,
                data: new TxResult(await this.ExecuteRW(tx.Data, tx.Size)),
                size: Consts.AvgMessageSize
            ));
        }
    
        public ICloneable GetFromStorage(string key)
        {
            return this.storage[key];
        }

        public bool HasInStorage(string key)
        {
            return this.storage.ContainsKey(key);
        }
    }
}