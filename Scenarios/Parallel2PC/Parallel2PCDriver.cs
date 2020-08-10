using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Transactions.Infrastructure.Network;
using Transactions.Infrastructure;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;

namespace Transactions.Scenarios.Parallel2PC
{
    public static class Parallel2PCDriver
    {
        public static void Run()
        {
            var networkSpec = Consts.INTRA_DC_NETWORK;
            var ssdSpec = Consts.SLOW_SSD;
            var duration = new Microsecond(60 * 1000 * 1000);

            var backoffCapUs = ssdSpec.fsync.value * 5;
            var attemptsPerIncrease = 4;
            
            var driver = new TxDriver(
                networkSpec, ssdSpec,
                (network, clock, random, address, shardLocator, ssd) => new DbNode(network, clock, random, address, ssd),
                (network, clock, random, address, shardLocator, appLocator) => new Parallel2PCAppNode(network, clock, random, address, shardLocator, (long)backoffCapUs, attemptsPerIncrease),
                (network, clock, random, address, shardLocator) => new Parallel2PCInitNode(network, clock, random, address, shardLocator)
            );
            
            var stat = new Stat();

            // 10*30 80+20 / 300 : 100
            //             / 30 : 10 (8+2)
            
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
            stat.ExportDuration("read", "pl2pc.read-tx.dist");
            stat.ExportDuration("transfer", "pl2pc.transfer-tx.dist");
            stat.Plot("jeka.pl2pc.png");
        }

        private static string Run(IOSpec networkSpec, SSDSpec ssdSpec, int clientCount, Microsecond duration)
        {
            var backoffCapUs = ssdSpec.fsync.value * 5;
            var attemptsPerIncrease = 4;
            
            var driver = new TxDriver(
                networkSpec, ssdSpec,
                (network, clock, random, address, shardLocator, ssd) => new DbNode(network, clock, random, address, ssd),
                (network, clock, random, address, shardLocator, appLocator) => new Parallel2PCAppNode(network, clock, random, address, shardLocator, (long)backoffCapUs, attemptsPerIncrease),
                (network, clock, random, address, shardLocator) => new Parallel2PCInitNode(network, clock, random, address, shardLocator)
            );
            
            var stat = new Stat();
            
            driver.MakeExperimentWithUniformConflicts(stat: stat, shardCount: 10, keysPerShard: 30, clientCount: clientCount, readRatio: 4, transferRatio: 1, duration: duration);

            stat.Sort();

            var throughput = stat.GetThroughput();
            var work = stat.GetAmoutOfWorkDone();

            var rmax = stat.Max("read");
            var rp99 = stat.TxDurationPercentile("read", 0.99);
            var rp95 = stat.TxDurationPercentile("read", 0.95);
            var rp50 = stat.TxDurationPercentile("read", 0.5);
            var rmin = stat.Min("read");

            var tmax = stat.Max("transfer");
            var tp99 = stat.TxDurationPercentile("transfer", 0.99);
            var tp95 = stat.TxDurationPercentile("transfer", 0.95);
            var tp50 = stat.TxDurationPercentile("transfer", 0.5);
            var tmin = stat.Min("transfer");

            return $"{clientCount}\t{throughput}\t{work}\t{rmax}\t{rp99}\t{rp95}\t{rp50}\t{rmin}\t{tmax}\t{tp99}\t{tp95}\t{tp50}\t{tmin}";
        }
    
        public static void ExploreDynamics(string name, Microsecond duration, int fromClients, int toClients, int step)
        {
            using (var writer = new StreamWriter(name, true))
            {
                for (var i=fromClients;i<=toClients;i+=step)
                {
                    Console.WriteLine($"\ttesting #{i} clients");
                    var stat = Run(Consts.INTRA_DC_NETWORK, Consts.SLOW_SSD, i, duration);
                    Console.WriteLine(stat);
                    writer.WriteLine(stat);
                    writer.Flush();
                }
            }
        }
    }
}