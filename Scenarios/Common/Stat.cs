using System;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
using Transactions.Infrastructure;

namespace Transactions.Scenarios.Common
{
    public class Stat
    {
        private Dictionary<string, List<ulong>> txDurations = new Dictionary<string, List<ulong>>();
        private Dictionary<string, List<ulong>> tracks = new Dictionary<string, List<ulong>>();
        private Microsecond? started = null;
        private Microsecond? stopped = null;

        public void ExpectClients(HashSet<string> clients)
        {
            foreach (var client in clients)
            {
                tracks.Add(client, new List<ulong>());
            }
        }
        
        public void StartTracking(Microsecond started)
        {
            this.started = started;
        }
        public void StopTracking(Microsecond stopped)
        {
            var clients = this.ongoing.Keys.ToList();
            foreach (var client in clients)
            {
                StopTx(client, stopped.value);
            }

            this.stopped = stopped;
        }

        public Dictionary<string, int> CountSummary()
        {
            var d = new Dictionary<string, long>();
            return tracks.ToDictionary(x => x.Key, x => x.Value.Count / 2);
        }

        
        private class OpInfo { public string type; public ulong started; };
        private Dictionary<string, OpInfo> ongoing = new Dictionary<string, OpInfo>();

        public void StartTx(string type, string client, ulong started)
        {
            if (this.stopped.HasValue) return;

            this.ongoing.Add(client, new OpInfo { type = type, started = started });
        }

        public void StopTx(string client, ulong stopped)
        {
            if (this.stopped.HasValue) return;
            
            var op = this.ongoing[client];
            this.ongoing.Remove(client);

            if (!this.started.HasValue)
            {
                return;
            }

            AddTxDuration(op.type, client, op.started, stopped);
        }
        
        private void AddTxDuration(string type, string client, ulong started, ulong ended)
        {
            if (!txDurations.ContainsKey(type))
            {
                txDurations.Add(type, new List<ulong>());
            }

            txDurations[type].Add(ended - started);

            tracks[client].Add(started);
            tracks[client].Add(ended);
        }

        public void Plot(string fileName)
        {
            ulong minTxLen = ulong.MaxValue;
            foreach(var type in txDurations.Keys)
            {
                minTxLen = Math.Min(
                    minTxLen,
                    txDurations[type][txDurations[type].Count - 1]
                );
            }

            ulong maxTime = ulong.MinValue;
            foreach(var client in tracks.Keys)
            {
                maxTime = Math.Max(
                    maxTime,
                    tracks[client][tracks[client].Count-1]
                );
            }

            minTxLen = minTxLen / 3;

            int width = (int)(maxTime / minTxLen);
            int height = tracks.Count;

            // Creates a new image with empty pixel data. 
            using(var image = new Image<Rgba32>(width, height)) 
            {
                var clients = new List<string>(tracks.Keys);
                var txPos = new Dictionary<int, int[]>();

                for (var t=0; t<width; t++)
                {
                    for (var y=0;y<height;y++)
                    {
                        if (!txPos.ContainsKey(y))
                        {
                            txPos.Add(y, new[] { 0, 0});
                        }

                        while (true)
                        {
                            ulong txb;
                            ulong txe;
                            
                            if (txPos[y][0] >= tracks[clients[y]].Count)
                            {
                                image[t, y] = new Rgba32(0, 255, 0);
                                break;
                            }
                            else
                            {
                                try
                                {
                                    txb = tracks[clients[y]][txPos[y][0]];
                                    txe = tracks[clients[y]][txPos[y][0]+1];
                                }
                                catch(Exception)
                                {
                                    Console.WriteLine($"Count: {tracks[clients[y]]}");
                                    Console.WriteLine($"Pos: {txPos[y][0]}");
                                    throw;
                                }
                            }

                            ulong b = minTxLen * (ulong)t;
                            ulong e = minTxLen * ((ulong)t + 1);

                            if (txb >= e)
                            {
                                image[t, y] = new Rgba32(0, 255, 0);
                                break;
                            }

                            if (txe < b)
                            {
                                txPos[y][0] += 2;
                                continue;
                            }

                            if (txb == b && t!=0)
                            {
                                txPos[y][1] = 1 - txPos[y][1];
                            }

                            if (txb <= b)
                            {
                                if (txe >= e)
                                {
                                    image[t, y] = txPos[y][1] == 0 ? new Rgba32(0, 0, 0) : new Rgba32(255, 255, 255);

                                    if (txe == e)
                                    {
                                        txPos[y][1] = 1 - txPos[y][1];
                                    }

                                    break;
                                }

                                var k = ((double)(txe - b)) / minTxLen;
                                var c = txPos[y][1] == 0 ? (byte)(255 * (1.0 - k)) : (byte)(255 * k);
                                image[t, y] = new Rgba32(c, c, c);
                                txPos[y][1] = 1 - txPos[y][1];
                            }
                            else
                            {
                                txPos[y][1] = 1 - txPos[y][1];
                                var k = ((double)(e - txb)) / minTxLen;
                                var c = txPos[y][1] == 0 ? (byte)(255 * (1.0 - k)) : (byte)(255 * k);
                                image[t, y] = new Rgba32(c, c, c);
                            }

                            break;
                        }
                    }
                }
                
                image.Save(fileName);
            }
        }

        public void Sort()
        {
            foreach(var type in txDurations.Keys)
            {
                txDurations[type].Sort();
                txDurations[type].Reverse();
            }
        }

        public ulong GetThroughput()
        {
            return (ulong)1000000 * this.GetAmoutOfWorkDone() / (stopped.Value.value - started.Value.value);
        }

        public ulong GetAmoutOfWorkDone()
        {
            long result = 0;
            foreach (var typeWork in txDurations.Select(x => x.Value.Count))
            {
                result += typeWork;
            }
            return (ulong)result;
        }
        
        public ulong Max(string type)
        {
            return txDurations[type][0];
        }

        public ulong Min(string type)
        {
            return txDurations[type][txDurations[type].Count - 1];
        }
        
        public ulong TxDurationPercentile(string type, double rank)
        {
            var index = (int)(txDurations[type].Count * (1 - rank));
            if (index == txDurations[type].Count)
            {
                index--;
            }
            return txDurations[type][index];
        }

        public void ExportDuration(string type, string fileName)
        {
            using (var stream = new StreamWriter(fileName, false))
            {
                foreach (var measure in txDurations[type])
                {
                    stream.WriteLine(measure);
                }
            }
        }
    }
}