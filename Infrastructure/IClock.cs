using System;
using System.Threading.Tasks;

namespace Transactions.Infrastructure
{
    public interface IClock
    {
        Microsecond Now { get; }

        void Delay(Microsecond delay, Action action);

        Task Delay(Microsecond delay);

        // Now -> Milliseconds
        // Delay(Microsecond)
    }
}