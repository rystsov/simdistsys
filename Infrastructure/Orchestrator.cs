using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Transactions.Infrastructure
{
    public class Orchestrator : SynchronizationContext, IClock, IRandom
    {
        private readonly object monitor = new object();
        private bool isActive = true;
        private readonly Queue<Action> continuations = new Queue<Action>();
        private readonly Random random = new Random();
        private readonly MinHeap<Action> heap = new MinHeap<Action>();

        public Microsecond Now { get; private set; } = new Microsecond(0);

        public int Next(int max)
        {
            return random.Next(max);
        }

        public int Next()
        {
            return random.Next();
        }

        public double NextDouble()
        {
            return random.NextDouble();
        }
        
        public void Delay(Microsecond delay, Action action)
        {
            lock (this.monitor)
            {
                if (Now.value + delay.value < Now.value)
                {
                    Console.WriteLine("If only I could turn back time");
                    throw new Exception("If only I could turn back time");
                }

                heap.Push(action, Now.value + delay.value);
                Monitor.Pulse(this.monitor);
            }
        }

        public Task Delay(Microsecond delay)
        {
            var tcs = new TaskCompletionSource<bool>();
            this.Delay(delay, () => tcs.SetResult(true));
            return tcs.Task;
        }

        public override void Send(SendOrPostCallback codeToRun, object state)
        {
            throw new NotImplementedException();
        }

        public override void Post(SendOrPostCallback continuation, object state)
        {
            lock (this.monitor)
            {
                continuations.Enqueue(() => continuation(state));
                Monitor.Pulse(this.monitor);
            }
        }

        public void EventLoop()
        {
            while (true)
            {
                Action continuation = null;
                lock(this.monitor)
                {
                    while (this.isActive && this.continuations.Count == 0 && this.heap.Count == 0)
                    {
                        Monitor.Wait(this.monitor);
                    }
                    
                    if (!this.isActive)
                    {
                        return;
                    }
                    else if (this.continuations.Count > 0)
                    {
                        continuation = continuations.Dequeue();
                    }
                    else if (this.heap.Count > 0)
                    {
                        this.Now = new Microsecond(heap.Min());
                        continuation = this.heap.Pop();
                    }
                }

                continuation();
            }
        }

        public void Stop()
        {
            lock (this.monitor)
            {
                this.isActive = false;
                Monitor.Pulse(this.monitor);
            }
        }
    }
}