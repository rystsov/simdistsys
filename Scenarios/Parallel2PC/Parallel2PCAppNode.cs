using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using Transactions.Infrastructure;
using Transactions.Infrastructure.Network;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;
using Transactions.Scenarios.Common.Messages;

namespace Transactions.Scenarios.Parallel2PC
{
    public class Parallel2PCAppNode : Node, IAppNode
    {
        private readonly Parallel2PCCore core;
        
        public Parallel2PCAppNode(IEndpoint network, IClock clock, IRandom random, string address, Func<string, string> shardLocator, long backoffCapUs, int attemptsPerIncrease)
            : base(network, clock, random, address)
        {
            this.core = new Parallel2PCCore(this, shardLocator, true, backoffCapUs, attemptsPerIncrease);
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
                else if (this.replies.ContainsKey(message.ID))
                {
                    var pr = this.replies[message.ID];
                    this.replies.Remove(message.ID);
                    pr.SetResult(message);
                }
                else
                {
                    Console.WriteLine($"{nameof(Parallel2PCAppNode)}: Unexpected message: {message.GetType().FullName}");
                    throw new Exception();
                }
            }
        }

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
    }
}