using Transactions.Infrastructure.Network;
using Transactions.Infrastructure;

namespace Transactions.Infrastructure.Network
{
    public class IOSpec
    {
        public Microsecond latency;
        public Microsecond noise;
        public BytesPerMicrosecond throughput;
    }
}