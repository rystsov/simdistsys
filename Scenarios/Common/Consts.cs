using System;
using System.Threading;
using System.Threading.Tasks;
using Transactions.Infrastructure.Network;
using Transactions.Infrastructure;

namespace Transactions.Scenarios.Common
{
    public static class Consts
    {
        public static uint AvgMessageSize = 1024;

        // 36 is min, 36+17 max, 0-17 uniform dist
        // 5 Gbps is 625 MB/s is 655 bytes per microsecond
        public readonly static IOSpec INTRA_DC_NETWORK = new IOSpec
        {
            latency = new Microsecond(36),
            noise = new Microsecond(17),
            throughput = new BytesPerMicrosecond(655)
        };

        // https://www.percona.com/blog/2018/02/08/fsync-performance-storage-devices/
        // https://ark.intel.com/content/www/us/en/ark/products/79624/intel-ssd-dc-p3700-series-400gb-1-2-height-pcie-3-0-20nm-mlc.html
        // 1080 MB/s ~ 1132 bytes per microseconds
        public readonly static SSDSpec NOMINAL_SSD = new SSDSpec
        {
            latency = new Microsecond(30),
            fsync = new Microsecond(140),
            throughput = new BytesPerMicrosecond(1132)
        };

        // https://twitter.com/emaxerrno/status/1256726511991713792
        public readonly static SSDSpec SLOW_SSD = new SSDSpec
        {
            latency = new Microsecond(30),
            fsync = new Microsecond(2000),
            throughput = new BytesPerMicrosecond(1132)
        };
    }
}