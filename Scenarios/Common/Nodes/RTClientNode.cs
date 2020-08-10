using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.Common.Nodes
{
    public class RTClientNode : Node
    {
        public class InsufficientFundsException : Exception
        {
            public Dictionary<string, int> Accounts { get; set; }
        }
        
        private readonly Dictionary<string, TaskCompletionSource<IMessage>> expectedRequests = new Dictionary<string, TaskCompletionSource<IMessage>>();
        private readonly Stat stat;
        private readonly int readRatio;
        private readonly int writeRatio;
        
        public RTClientNode(IEndpoint network, IClock clock, IRandom random, string address,  Stat stat, int readRatio, int writeRatio)
            : base(network, clock, random, address)
        {
            this.stat = stat;
            this.readRatio = readRatio;
            this.writeRatio = writeRatio;
        }

        public async Task Run(HashSet<string> appAddresses, Func<string> keyProvider, int delta)
        {
            var apps = new List<string>(appAddresses);
            
            var elth = StartEventLoop();

            while (true)
            {
                var key1 = keyProvider();
                var key2 = keyProvider();

                if (key1 == key2) continue;

                var app = apps[this.random.Next(apps.Count)];
                
                if (this.random.Next(readRatio + writeRatio) < readRatio)
                {
                    this.stat.StartTx("read", this.address, this.clock.Now.value);
                    await this.Read(app, new HashSet<string>{key1, key2});
                    this.stat.StopTx(this.address, this.clock.Now.value);
                }
                else
                {
                    try
                    {
                        this.stat.StartTx("transfer", this.address, this.clock.Now.value);
                        await this.Transfer(app, key1, key2, delta);
                        this.stat.StopTx(this.address, this.clock.Now.value);
                    }
                    catch(InsufficientFundsException)
                    {
                        this.stat.StopTx(this.address, this.clock.Now.value);
                    }
                }
            }
        }

        private async Task<Dictionary<string, int>> Read(string appAddress, HashSet<string> accounts)
        {
            var txid = Guid.NewGuid().ToString();
            var expectation = new TaskCompletionSource<IMessage>();
            this.expectedRequests.Add(txid, expectation);
            var tx = new AppMessages.ReadTx();
            tx.Accounts.AddRange(accounts);
            await this.network.SendAsync(new DataMessage<AppMessages.ReadTx>(
                dest: appAddress,
                source: this.address,
                id: txid,
                data: tx,
                size: Consts.AvgMessageSize
            ));
            var result = (DataMessage<AppMessages.TxOk>)(await expectation.Task);
            return result.Data.Accounts;
        }

        private async Task<Dictionary<string, int>> Transfer(string appAddress, string donor, string recipient, int delta)
        {
            var txid = Guid.NewGuid().ToString();
            var expectation = new TaskCompletionSource<IMessage>();
            this.expectedRequests.Add(txid, expectation);

            var tx = new AppMessages.TransferTx(donor, recipient, delta);
            await this.network.SendAsync(new DataMessage<AppMessages.TransferTx>(
                dest: appAddress,
                source: this.address,
                id: txid,
                data: tx,
                size: Consts.AvgMessageSize
            ));
            var result = await expectation.Task;

            if (result is DataMessage<AppMessages.TxOk> txok)
            {
                return txok.Data.Accounts;
            }
            else if (result is DataMessage<AppMessages.TxInsufficientFunds> txins)
            {
                throw new InsufficientFundsException()
                {
                    Accounts = txins.Data.Accounts
                };
            }
            else
            {
                throw new Exception();
            }
        }
        
        private async Task StartEventLoop()
        {
            while (true)
            {
                var message = await this.network.ReceiveAsync();

                if (expectedRequests.ContainsKey(message.ID))
                {
                    var expecting = expectedRequests[message.ID];
                    expectedRequests.Remove(message.ID);
                    expecting.SetResult(message);
                }
            }
        }
    }
}