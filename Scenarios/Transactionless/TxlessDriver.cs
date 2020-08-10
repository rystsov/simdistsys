using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Transactions.Infrastructure.Network;
using Transactions.Infrastructure;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;

namespace Transactions.Scenarios.Transactionless
{
    public static class TxlessDriver
    {
        public static void Run()
        {
            var networkSpec = Consts.INTRA_DC_NETWORK;
            var ssdSpec = Consts.SLOW_SSD;
            var duration = new Microsecond(60 * 1000 * 1000);
            
            var driver = new TxDriver(
                networkSpec, ssdSpec,
                (network, clock, random, address, _, ssd) => new DbNode(network, clock, random, address, ssd),
                (network, clock, random, address, shardLocator, _) => new AppNode(network, clock, random, address, shardLocator),
                (network, clock, random, address, shardLocator) => new InitNode(network, clock, random, address, shardLocator)
            );
            
            var stat = new Stat();
            
            driver.MakeExperimentWithUniformConflicts(stat: stat, shardCount: 10, keysPerShard: 30, clientCount: 100, readRatio: 4, transferRatio: 1, duration: duration);

            stat.Sort();
            Console.WriteLine($"Throuthput (tps): {stat.GetThroughput()}");
            Console.WriteLine($"Work (ts): {stat.GetAmoutOfWorkDone()}");

            Console.WriteLine("Reads:");
            Console.WriteLine($"\tmax: {stat.Max("read")} / min: {stat.Min("read")}");
            Console.WriteLine($"\tp99: {stat.TxDurationPercentile("read", 0.99)}");
            Console.WriteLine($"\tp95: {stat.TxDurationPercentile("read", 0.95)}");
            Console.WriteLine($"\tp50: {stat.TxDurationPercentile("read", 0.5)}");
            Console.WriteLine();
            
            Console.WriteLine("Transfers:");
            Console.WriteLine($"\tmax: {stat.Max("transfer")} / min: {stat.Min("transfer")}");
            Console.WriteLine($"\tp99: {stat.TxDurationPercentile("transfer", 0.99)}");
            Console.WriteLine($"\tp95: {stat.TxDurationPercentile("transfer", 0.95)}");
            Console.WriteLine($"\tp50: {stat.TxDurationPercentile("transfer", 0.5)}");
            stat.ExportDuration("read", "txless.read-tx.dist");
            stat.ExportDuration("transfer", "txless.transfer-tx.dist");
            stat.Plot("jeka.txless.png");
        }
    }
}