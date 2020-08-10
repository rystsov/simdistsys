using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Transactions.Infrastructure.Network
{
    public class Simplex<T> : ISimplex<T> where T : IMessage
    {
        private class InflightMessage
        {
            public T message;
            public Microsecond receivedAt;
            public Microsecond noticedAt;
        }

        private class Leftover
        {
            public InflightMessage message;
            public ulong bytesSent;
        }
        
        private readonly BytesPerMicrosecond throuthput;
        private readonly Microsecond latency;
        private readonly Microsecond variance;
        private readonly IClock clock;
        private readonly IRandom random;

        private readonly Queue<InflightMessage> incomming = new Queue<InflightMessage>();
        private readonly Queue<InflightMessage> pending = new Queue<InflightMessage>();
        private bool isSending = false;

        private readonly Queue<T> delivered = new Queue<T>();
        private readonly Queue<TaskCompletionSource<T>> receivers = new Queue<TaskCompletionSource<T>>();
        
        public Simplex(IClock clock, IRandom random, BytesPerMicrosecond throuthput, Microsecond latency, Microsecond variance)
        {
            this.clock = clock;
            this.random = random;
            this.throuthput = throuthput;
            this.latency = latency;
            this.variance = variance;
        }

        public Task SendAsync(T message)
        {
            this.incomming.Enqueue(new InflightMessage
            {
                message = message,
                receivedAt = this.clock.Now
            });

            _ = this.Send();

            return Task.CompletedTask;
        }

        private async Task Send()
        {
            if (this.isSending)
            {
                return;
            }

            var delay = this.latency.value / 2;
            var batchSize = this.throuthput.value * delay;

            this.isSending = true;
            Leftover leftover = null;

            while (leftover != null || this.incomming.Count > 0 || this.pending.Count > 0)
            {
                while (this.incomming.Count > 0)
                {
                    var message = this.incomming.Dequeue();
                    message.noticedAt = this.clock.Now;
                    this.pending.Enqueue(message);
                }
                
                var frame = new List<InflightMessage>();
                ulong frameSize = 0;
                
                if (leftover != null)
                {
                    if (leftover.message.message.Size - leftover.bytesSent < batchSize)
                    {
                        frame.Add(leftover.message);
                        frameSize += leftover.message.message.Size - leftover.bytesSent;
                        leftover = null;
                    }
                    else
                    {
                        leftover.bytesSent += batchSize;
                        await this.clock.Delay(new Microsecond(delay));
                        continue;
                    }
                }
                
                while (this.pending.Any() && frameSize + this.pending.Peek().message.Size < batchSize)
                {
                    var message = this.pending.Dequeue();
                    frame.Add(message);
                    frameSize += message.message.Size;
                }

                if (this.pending.Any())
                {
                    leftover = new Leftover
                    {
                        message = this.pending.Dequeue(),
                        bytesSent = batchSize - frameSize
                    };
                }

                await this.clock.Delay(new Microsecond(delay));

                foreach(var envelope in frame)
                {
                    var message = envelope.message;
                    var expectedLatency = this.latency.value + (ulong)this.random.Next((int)this.variance.value);
                    var actualLatency = delay + (envelope.noticedAt.value - envelope.receivedAt.value);
                    if (actualLatency < expectedLatency)
                    {
                        this.clock.Delay(
                            new Microsecond(expectedLatency - actualLatency),
                            delegate
                            {
                                this.delivered.Enqueue(message);
                                while (this.delivered.Count > 0 && this.receivers.Count > 0)
                                {
                                    this.receivers.Dequeue().SetResult(this.delivered.Dequeue());
                                }
                            }
                        );
                    }
                    else
                    {
                        this.delivered.Enqueue(message);
                        while (this.delivered.Count > 0 && this.receivers.Count > 0)
                        {
                            this.receivers.Dequeue().SetResult(this.delivered.Dequeue());
                        }
                    }
                }
            }

            this.isSending = false;
        }

        public Task<T> ReceiveAsync()
        {
            var tcs = new TaskCompletionSource<T>();
            
            if (this.delivered.Count == 0)
            {
                this.receivers.Enqueue(tcs);
            }
            else
            {
                tcs.SetResult(this.delivered.Dequeue());
            }

            return tcs.Task;
        }
    }
}