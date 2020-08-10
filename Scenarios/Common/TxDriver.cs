using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Transactions.Infrastructure.Network;
using Transactions.Infrastructure;
using Transactions.Scenarios.Common;
using Transactions.Scenarios.Common.Nodes;

namespace Transactions.Scenarios.Common
{
    public class TxDriver
    {
        private readonly DbNodeFactory dbNodeFactory;
        private readonly InitNodeFactory initNodeFactory;
        private readonly AppNodeFactory appNodeFactory;
        private readonly IOSpec network;
        private readonly SSDSpec ssd;

        public TxDriver(IOSpec network, SSDSpec ssd, DbNodeFactory dbNodeFactory, AppNodeFactory appNodeFactory, InitNodeFactory initNodeFactory)
        {
            this.dbNodeFactory = dbNodeFactory;
            this.appNodeFactory = appNodeFactory;
            this.initNodeFactory = initNodeFactory;
            this.network = network;
            this.ssd = ssd;
        }

        public void MakeExperimentWithUniformConflicts(Stat stat, int shardCount, int keysPerShard, int clientCount, int readRatio, int transferRatio, Microsecond duration)
        {
            var orchestrator = new Orchestrator();
            SynchronizationContext.SetSynchronizationContext(orchestrator);
            orchestrator.Post(x => _ = RunWithConflicts(orchestrator: orchestrator, stat: stat, distrib: UniformRandomGenerator, shardCount: shardCount, keysPerShard: keysPerShard, clientCount: clientCount, readRatio: readRatio, transferRatio: transferRatio, duration: duration), null);
            orchestrator.EventLoop();
        }

        public void MakeExperimentWithZipfConflicts(Stat stat, int shardCount, int keysPerShard, int clientCount, int readRatio, int transferRatio, Microsecond duration)
        {
            var orchestrator = new Orchestrator();
            SynchronizationContext.SetSynchronizationContext(orchestrator);
            orchestrator.Post(x => _ = RunWithConflicts(orchestrator: orchestrator, stat: stat, distrib: ZipfRandomGenerator, shardCount: shardCount, keysPerShard: keysPerShard, clientCount: clientCount, readRatio: readRatio, transferRatio: transferRatio, duration: duration), null);
            orchestrator.EventLoop();
        }

        public void MakeExperimentWithoutConflicts(Stat stat, int shardCount, int keysPerShard, int clientCount, int readRatio, int transferRatio, Microsecond duration)
        {
            var orchestrator = new Orchestrator();
            SynchronizationContext.SetSynchronizationContext(orchestrator);
            orchestrator.Post(x => _ = RunWithoutConflicts(orchestrator: orchestrator, stat: stat, shardCount: shardCount, keysPerShard: keysPerShard, clientCount: clientCount, readRatio: readRatio, transferRatio: transferRatio, duration: duration), null);
            orchestrator.EventLoop();
        }

        private async Task DumpStat(IClock clock, Stat stat)
        {
            var prev = DateTime.Now;
            while (true)
            {
                var summary = stat.CountSummary();
                var min = int.MaxValue;
                var max = int.MinValue;
                foreach (var client in summary.Keys)
                {
                    min = Math.Min(min, summary[client]);
                    max = Math.Max(max, summary[client]);
                    //Console.WriteLine($"{client}: {summary[client]}");
                }
                var curr = DateTime.Now;
                var took = (curr - prev).TotalSeconds;
                Console.WriteLine($"{curr} |keys|: {summary.Count} min: {min} max: {max}; 1s took {took}s");
                await clock.Delay(new Microsecond(1 * 1000* 1000));
                prev = curr;
            }
        }

        private Func<string> UniformRandomGenerator(IRandom random, HashSet<string> keys)
        {
            var data = keys.ToArray();
            return delegate()
            {
                return data[random.Next(data.Length)];
            };
        }

        private Func<string> ZipfRandomGenerator(IRandom random, HashSet<string> keys)
        {
            var data = keys.OrderBy(x => random.Next()).ToArray();
            var zipf = new ZipfRandom(random, 1.1, data.Length);

            return delegate()
            {
                return data[zipf.RandomRank()];
            };
        }

        private async Task RunWithConflicts(Orchestrator orchestrator, Stat stat, Func<IRandom, HashSet<string>, Func<string>> distrib, int shardCount, int keysPerShard, int clientCount, int readRatio, int transferRatio, Microsecond duration)
        {
            try
            {
                int initMoneyAmount = 10000;
                int tdelta = 10;
                
                var keyShardMapping = new Dictionary<string, string>();
                var keyAppMapping = new Dictionary<string, string>();
                for (var shard=0; shard < shardCount; shard++)
                {
                    for (var key=0; key < keysPerShard; key++)
                    {
                        keyShardMapping.Add($"key{shard}:{key}", $"shard{shard}");
                        keyAppMapping.Add($"key{shard}:{key}", $"app{shard}");
                    }
                }

                Func<string, string> shardLocator = delegate(string key)
                {
                    if (!keyShardMapping.ContainsKey(key))
                    {
                        return $"shard{Math.Abs(key.GetHashCode()) % shardCount}";
                    }

                    return keyShardMapping[key];
                };

                Func<string, string> appLocator = delegate(string key)
                {
                    if (!keyAppMapping.ContainsKey(key))
                    {
                        return $"app{Math.Abs(key.GetHashCode()) % shardCount}";
                    }

                    return keyAppMapping[key];
                };
                
                var router = new Switch();
                
                var initCord = new Duplex(orchestrator, network);
                router.RegisterRoute("init", initCord.Endpoint1);

                var init = this.initNodeFactory(initCord.Endpoint2, orchestrator, orchestrator, "init", shardLocator);

                var shards = new Dictionary<string, IDbNode>();
                var apps = new Dictionary<string, IAppNode>();
                for (var i=0;i<shardCount;i++)
                {
                    var shardName = $"shard{i}";
                    var shardCord = new Duplex(orchestrator, network);
                    router.RegisterRoute(shardName, shardCord.Endpoint1);
                    var shard = this.dbNodeFactory(shardCord.Endpoint2, orchestrator, orchestrator, shardName, shardLocator, ssd);
                    shards.Add(shardName, shard);

                    var appName = $"app{i}";
                    var appCord = new Duplex(orchestrator, network);
                    router.RegisterRoute(appName, appCord.Endpoint1);
                    var app = this.appNodeFactory(appCord.Endpoint2, orchestrator, orchestrator, appName, shardLocator, appLocator);
                    apps.Add(appName, app);
                }

                var clients = new Dictionary<string, RTClientNode>();
                for (var i=0;i<clientCount;i++)
                {
                    var clientName = $"client{i}";
                    var clientCord = new Duplex(orchestrator, network);
                    router.RegisterRoute(clientName, clientCord.Endpoint1);
                    var client = new RTClientNode(clientCord.Endpoint2, orchestrator, orchestrator, clientName, stat, readRatio, transferRatio);
                    clients.Add(clientName, client);
                }

                stat.ExpectClients(clients.Keys.ToHashSet());

                var routerThread = router.Run();

                foreach (var shard in shards.Keys)
                {
                    _ = shards[shard].Run();
                }

                foreach (var app in apps.Keys)
                {
                    _ = apps[app].Run();
                }

                var initState = new Dictionary<string, int>();
                foreach(var key in keyShardMapping.Keys)
                {
                    initState.Add(key, initMoneyAmount);
                }
                await init.Run(initState);

                var started = orchestrator.Now.value;
                Console.WriteLine(started);

                var keyGen = distrib(orchestrator, keyShardMapping.Keys.ToHashSet());

                foreach (var client in clients.Keys)
                {
                    _ = clients[client].Run(new HashSet<string>(apps.Keys), keyGen, tdelta);
                }

                _ = this.DumpStat(orchestrator, stat);

                // 1s to warm up
                await orchestrator.Delay(new Microsecond(1 * 1000 * 1000));
                stat.StartTracking(orchestrator.Now);
                await orchestrator.Delay(duration);
                stat.StopTracking(orchestrator.Now);

                var ended = orchestrator.Now.value;
                Console.WriteLine(ended);
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                Console.WriteLine("Done");
                orchestrator.Stop();
            }
        }
    
        private async Task RunWithoutConflicts(Orchestrator orchestrator, Stat stat, int shardCount, int keysPerShard, int clientCount, int readRatio, int transferRatio, Microsecond duration)
        {
            try
            {
                int initMoneyAmount = 10000;
                int tdelta = 10;
                
                var keyShardMapping = new Dictionary<string, string>();
                var keyAppMapping = new Dictionary<string, string>();
                var keysPerClient = new Dictionary<string, HashSet<string>>();
                for (var client = 0; client < clientCount; client++)
                {
                    var clientName = $"client{client}";
                    keysPerClient.Add(clientName, new HashSet<string>());
                    for (var shard=0; shard < shardCount; shard++)
                    {
                        for (var key=0; key < keysPerShard; key++)
                        {
                            var keyName = $"key{client}:{shard}:{key}";
                            keyShardMapping.Add(keyName, $"shard{shard}");
                            keyAppMapping.Add(keyName, $"app{shard}");
                            keysPerClient[clientName].Add(keyName);
                        }
                    }
                }

                Func<string, string> shardLocator = delegate(string key)
                {
                    if (!keyShardMapping.ContainsKey(key))
                    {
                        return $"shard{Math.Abs(key.GetHashCode()) % shardCount}";
                    }

                    return keyShardMapping[key];
                };

                Func<string, string> appLocator = delegate(string key)
                {
                    if (!keyAppMapping.ContainsKey(key))
                    {
                        return $"app{Math.Abs(key.GetHashCode()) % shardCount}";
                    }

                    return keyAppMapping[key];
                };
                
                var router = new Switch();
                
                var initCord = new Duplex(orchestrator, network);
                router.RegisterRoute("init", initCord.Endpoint1);

                var init = this.initNodeFactory(initCord.Endpoint2, orchestrator, orchestrator, "init", shardLocator);

                var shards = new Dictionary<string, IDbNode>();
                var apps = new Dictionary<string, IAppNode>();
                for (var i=0;i<shardCount;i++)
                {
                    var shardName = $"shard{i}";
                    var shardCord = new Duplex(orchestrator, network);
                    router.RegisterRoute(shardName, shardCord.Endpoint1);
                    var shard = this.dbNodeFactory(shardCord.Endpoint2, orchestrator, orchestrator, shardName, shardLocator, ssd);
                    shards.Add(shardName, shard);

                    var appName = $"app{i}";
                    var appCord = new Duplex(orchestrator, network);
                    router.RegisterRoute(appName, appCord.Endpoint1);
                    var app = this.appNodeFactory(appCord.Endpoint2, orchestrator, orchestrator, appName, shardLocator, appLocator);
                    apps.Add(appName, app);
                }

                var clients = new Dictionary<string, RTClientNode>();
                for (var i=0;i<clientCount;i++)
                {
                    var clientName = $"client{i}";
                    var clientCord = new Duplex(orchestrator, network);
                    router.RegisterRoute(clientName, clientCord.Endpoint1);
                    var client = new RTClientNode(clientCord.Endpoint2, orchestrator, orchestrator, clientName, stat, readRatio, transferRatio);
                    clients.Add(clientName, client);
                }

                stat.ExpectClients(clients.Keys.ToHashSet());

                var routerThread = router.Run();

                foreach (var shard in shards.Keys)
                {
                    _ = shards[shard].Run();
                }

                foreach (var app in apps.Keys)
                {
                    _ = apps[app].Run();
                }

                var initState = new Dictionary<string, int>();
                foreach(var key in keyShardMapping.Keys)
                {
                    initState.Add(key, initMoneyAmount);
                }
                await init.Run(initState);

                var started = orchestrator.Now.value;
                Console.WriteLine(started);

                foreach (var client in clients.Keys)
                {
                    _ = clients[client].Run(new HashSet<string>(apps.Keys), UniformRandomGenerator(orchestrator, keysPerClient[client]), tdelta);
                }

                _ = this.DumpStat(orchestrator, stat);

                // 1s to warm up
                await orchestrator.Delay(new Microsecond(1 * 1000 * 1000));
                stat.StartTracking(orchestrator.Now);
                await orchestrator.Delay(duration);
                stat.StopTracking(orchestrator.Now);

                var ended = orchestrator.Now.value;
                Console.WriteLine(ended);
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                Console.WriteLine("Done");
                orchestrator.Stop();
            }
        }
    }
}