using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Transactions.Infrastructure.Network
{
    public class SSD
    {
        private class Payload
        {
            public ulong size;
            public TaskCompletionSource<bool> tcs;
        }
        
        private readonly BytesPerMicrosecond throuthput;
        private readonly Microsecond latency;
        private readonly Microsecond fsync;
        private readonly IClock clock;

        private readonly Queue<Payload> incomming = new Queue<Payload>();
        private readonly Queue<Payload> written = new Queue<Payload>();
        private bool isWriting = false;
        private bool isSyncing = false;
        
        public SSD(IClock clock, SSDSpec spec)
        {
            this.clock = clock;
            this.throuthput = spec.throughput;
            this.latency = spec.latency;
            this.fsync = spec.fsync;
        }

        public Task WriteAsync(uint bytes)
        {
            var payload = new Payload
            {
                size = bytes,
                tcs = new TaskCompletionSource<bool>()
            };
            
            this.incomming.Enqueue(payload);

            _ = this.Process();

            return payload.tcs.Task;
        }

        private async Task Process()
        {
            if (this.isWriting)
            {
                return;
            }

            var batchSize = this.throuthput.value * this.latency.value;

            this.isWriting = true;

            while (this.incomming.Count > 0)
            {
                var frame = new Queue<Payload>();
                ulong frameSize = 0;
                
                while (this.incomming.Any() && frameSize + this.incomming.Peek().size < batchSize)
                {
                    var payload = this.incomming.Dequeue();
                    frame.Enqueue(payload);
                    frameSize += payload.size;
                }

                if (this.incomming.Any())
                {
                    var next = this.incomming.Peek();
                    next.size -= (batchSize - frameSize);
                }

                await this.clock.Delay(new Microsecond(this.latency.value));

                while (frame.Any())
                {
                    this.written.Enqueue(frame.Dequeue());
                }
                
                _ = Fsync();
            }

            this.isWriting = false;
        }

        private async Task Fsync()
        {
            if (this.isSyncing)
            {
                return;
            }

            this.isSyncing = true;

            while (this.written.Any())
            {
                var batch = new Queue<Payload>();

                while (this.written.Any())
                {
                    batch.Enqueue(this.written.Dequeue());
                }

                await this.clock.Delay(fsync);

                foreach(var payload in batch)
                {
                    payload.tcs.SetResult(true);
                }
            }

            this.isSyncing = false;
        }
    }
}