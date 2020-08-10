using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using Transactions.Scenarios.Transactionless;
using Transactions.Scenarios.Vanila2PC;
using Transactions.Scenarios.RW2PC;
using Transactions.Scenarios.OCC2PC;
using Transactions.Scenarios.Parallel2PC;
using Transactions.Scenarios.Mem.NW;
using Transactions.Scenarios.Mem.Rnd;
using Transactions.Scenarios.Mem.TS;
using Transactions.Infrastructure;

using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;


namespace Transactions
{
    class Program
    {
        enum Fault { none, single, continuous }

        static int Main(string[] args)
        {
            // Create a root command with some options
            var rootCommand = new RootCommand();
            rootCommand.Name = "simdistsys";
            rootCommand.Description = "Simulated playground for distributed algorithms";

            var paxos = new Command("consensus") {
                new Option<string>(
                    "--output",
                    getDefaultValue: () => "Plots/logs",
                    description: "Duration of an experiment in simulated seconds"
                ),
                new Option<int>(
                    "--duration-sec",
                    getDefaultValue: () => 10,
                    description: "Duration of an experiment in simulated seconds"
                ),
                new Option<int>(
                    "--from-clients",
                    getDefaultValue: () => 1,
                    description: "Lower number of clients"
                ),
                new Option<int>(
                    "--to-clients",
                    getDefaultValue: () => 20,
                    description: "Maximum number of clients"
                ),
                new Option<int>(
                    "--delta",
                    getDefaultValue: () => 1,
                    description: "Increase of clients between experiments"
                ),
                new Option<Fault>(
                    "--fault",
                    getDefaultValue: () => Fault.none,
                    description: "Fault injection strategy"
                ),
                new Option<string>(
                    "--leadership",
                    description: "Type of leadership"
                ) { IsRequired = true }.FromAmong("adhoc", "leaderless", "stable")
            };
            paxos.Handler = CommandHandler.Create<string, int, int, int, int, Fault, string>(PaxosExperiment);
            rootCommand.Add(paxos);

            var tx = new Command("tx");
            rootCommand.Add(tx);

            var uncoord = new Command("uncoordinated");
            uncoord.Description = "Without coordination there is a high risk of collision and extra work caused by retries";
            tx.Add(uncoord);

            uncoord.Add(UncoordinatedTxCommand("2pc", "two-phase commit (read & lock + execute + commit + release)"));
            uncoord.Add(UncoordinatedTxCommand("rw-2pc", "two-phase commit (read & rw-lock + execute + commit + release)"));
            uncoord.Add(UncoordinatedTxCommand("occ-2pc", "two-phase commit (read + execute + w-lock + commit + release)"));
            uncoord.Add(UncoordinatedTxCommand("occ-pl", "parallel commit (read + execute + w-lock & commit + release)"));

            var coord = new Command("coordinated");
            coord.Description = "In-memory lock + occ ondisk protocols";
            tx.Add(coord);

            coord.Add(CoordinatedTxCommand("occ-2pc", "occ two-phase commit"));
            coord.Add(CoordinatedTxCommand("occ-pl", "occ parallel commits"));

            // Parse the incoming args and invoke the handler
            return rootCommand.InvokeAsync(args).Result;
        }

        private static Command UncoordinatedTxCommand(string name, string description)
        {
            var cmd = new Command(name) {
                new Option<string>(
                    "--output",
                    getDefaultValue: () => "Plots/logs",
                    description: "Logs output"
                ),
                new Option<int>(
                    "--duration-sec",
                    getDefaultValue: () => 10,
                    description: "Duration of an experiment in simulated seconds"
                ),
                new Option<int>(
                    "--from-clients",
                    getDefaultValue: () => 1,
                    description: "Lower number of clients"
                ),
                new Option<int>(
                    "--to-clients",
                    getDefaultValue: () => 200,
                    description: "Maximum number of clients"
                ),
                new Option<int>(
                    "--delta",
                    getDefaultValue: () => 10,
                    description: "Increase of clients between experiments"
                )
            };
            cmd.Description = description;
            cmd.Handler = CommandHandler.Create<string, int, int, int, int>(
                (output,durationSec,fromClients,toClients,delta) => UncoordinatedTx(output, name, durationSec, fromClients, toClients, delta)
            );
            return cmd;
        }

        private static Command CoordinatedTxCommand(string name, string description)
        {
            var dp = name == "occ-2pc" ?
                new[] {"no-wait", "wait-die", "to"} : 
                new[] {"no-wait", "wait-die", "to", "random"};
            var cmd = new Command(name) {
                new Option<string>(
                    "--output",
                    getDefaultValue: () => "Plots/logs",
                    description: "Logs output"
                ),
                new Option<int>(
                    "--duration-sec",
                    getDefaultValue: () => 10,
                    description: "Duration of an experiment in simulated seconds"
                ),
                new Option<int>(
                    "--from-clients",
                    getDefaultValue: () => 1,
                    description: "Lower number of clients"
                ),
                new Option<int>(
                    "--to-clients",
                    getDefaultValue: () => 200,
                    description: "Maximum number of clients"
                ),
                new Option<int>(
                    "--delta",
                    getDefaultValue: () => 10,
                    description: "Increase of clients between experiments"
                ),
                new Option<string>(
                    "--deadlock-prevention",
                    description: "Increase of clients between experiments"
                ) { IsRequired = true }.FromAmong("no-wait", "wait-die", "to", "random")
            };
            cmd.Description = description;
            cmd.Handler = CommandHandler.Create<string, int, int, int, int, string>(
                (output, durationSec,fromClients,toClients,delta,deadlockPrevention) => 
                    CoordinatedTx(output, name, durationSec, fromClients, toClients, delta, deadlockPrevention)
            );
            return cmd;
        }
        
        static void UncoordinatedTx(string output, string name, int durationSec, int fromClients, int toClients, int delta)
        {
            var duration = new Microsecond((ulong)durationSec * 1000 * 1000);
            var log = Path.Combine(output, $"tx.uncoord.{name}");
            
            if (name == "2pc")
            {
                Vanila2PCDriver.ExploreDynamics(log, duration, fromClients, toClients, delta);
            }
            else if (name == "rw-2pc")
            {
                RW2PCDriver.ExploreDynamics(log, duration, fromClients, toClients, delta);
            }
            else if (name == "occ-2pc")
            {
                Parallel2PCDriver.ExploreDynamics(log, duration, fromClients, toClients, delta);
            }
            else if (name == "occ-pl")
            {
                OCC2PCDriver.ExploreDynamics(log, duration, fromClients, toClients, delta);
            }
            else
            {
                throw new ArgumentException(name);
            }
        }

        static void CoordinatedTx(string output, string name, int durationSec, int fromClients, int toClients, int delta, string deadlockPrevention)
        {
            var nwOccPcDriver = new NWDriver(
                (network, clock, random, address, shardLocator) => new Parallel2PCInitNode(network, clock, random, address, shardLocator),
                (node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease) => new Parallel2PCCore(node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease)
            );

            var wdOccPcDriver = new WDDriver(
                (network, clock, random, address, shardLocator) => new Parallel2PCInitNode(network, clock, random, address, shardLocator),
                (node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease) => new Parallel2PCCore(node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease)
            );

            var toOccPcDriver = new TODriver(
                (network, clock, random, address, shardLocator) => new Parallel2PCInitNode(network, clock, random, address, shardLocator),
                (node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease) => new Parallel2PCCore(node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease)
            );

            var nwOcc2PcDriver = new NWDriver(
                (network, clock, random, address, shardLocator) => new OCC2PCInitNode(network, clock, random, address, shardLocator),
                (node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease) => new OCC2PCCore(node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease)
            );

            var wdOcc2PcDriver = new WDDriver(
                (network, clock, random, address, shardLocator) => new OCC2PCInitNode(network, clock, random, address, shardLocator),
                (node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease) => new OCC2PCCore(node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease)
            );

            var toOcc2PcDriver = new TODriver(
                (network, clock, random, address, shardLocator) => new OCC2PCInitNode(network, clock, random, address, shardLocator),
                (node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease) => new OCC2PCCore(node, shardLocator, useBackoff, backoffCapUs, attemptsPerIncrease)
            );

            var duration = new Microsecond((ulong)durationSec * 1000 * 1000);

            // new[] {"no-wait", "wait-die", "to", "random"};

            // output

            var log = Path.Combine(output, $"tx.coord.{name}.{deadlockPrevention}");

            switch(deadlockPrevention)
            {
                case "no-wait":
                    if (name == "occ-pl")
                    {
                        nwOccPcDriver.ExploreDynamics(log, duration, fromClients, toClients, delta);
                    }
                    else if (name == "occ-2pc")
                    {
                        nwOcc2PcDriver.ExploreDynamics(log, duration, fromClients, toClients, delta);
                    }
                    else
                    {
                        throw new ArgumentException(name);
                    }
                    break;
                case "wait-die":
                    if (name == "occ-pl")
                    {
                        wdOccPcDriver.ExploreDynamics(log, duration, fromClients, toClients, delta, true);
                    }
                    else if (name == "occ-2pc")
                    {
                        wdOcc2PcDriver.ExploreDynamics(log, duration, fromClients, toClients, delta, true);
                    }
                    else
                    {
                        throw new ArgumentException(name);
                    }
                    break;
                case "to":
                    if (name == "occ-pl")
                    {
                        toOccPcDriver.ExploreDynamics(log, duration, fromClients, toClients, delta, true);
                    }
                    else if (name == "occ-2pc")
                    {
                        toOcc2PcDriver.ExploreDynamics(log, duration, fromClients, toClients, delta, true);
                    }
                    else
                    {
                        throw new ArgumentException(name);
                    }
                    break;
                case "random":
                    if (name == "occ-pl")
                    {
                        MemRndParallel2PCDriver.ExploreDynamics(log, duration, fromClients, toClients, delta);
                    }
                    else if (name == "occ-2pc")
                    {
                        throw new NotImplementedException();
                    }
                    else
                    {
                        throw new ArgumentException(name);
                    }
                    break;
            }
        }

        static void PaxosExperiment(string output, int durationSec, int fromClients, int toClients, int delta, Fault fault, string leadership)
        {
            if (!Directory.Exists(output))
            {
                Console.WriteLine($"Output directory ({output}) should exist");
                return;
            }
            
            Console.WriteLine("TODO: add paxos tests");
        }
    }
}
