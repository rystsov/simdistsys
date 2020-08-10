using Transactions.Infrastructure.Network;
using Transactions.Infrastructure;

namespace Transactions.Infrastructure.Network
{
    public class SSDSpec
    {
        public Microsecond latency;
        public Microsecond fsync;
        public BytesPerMicrosecond throughput;
    }
}