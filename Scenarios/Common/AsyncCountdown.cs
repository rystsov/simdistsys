using System;
using System.Threading;
using System.Threading.Tasks;

namespace Transactions.Scenarios.Common
{
    public class AsyncCountdown
    {
        private int n;
        private TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
        
        public AsyncCountdown(int initialCount)
        {
            if (initialCount <= 0) throw new Exception();
            this.n = initialCount;
        }

        public void Signal()
        {
            if (this.n <= 0) throw new Exception();
            this.n--;
            if (this.n == 0)
            {
                tcs.SetResult(true);
            }
        }

        public Task Wait()
        {
            return tcs.Task;
        }
    }
}