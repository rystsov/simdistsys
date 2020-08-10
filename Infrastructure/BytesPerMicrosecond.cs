using System;

namespace Transactions.Infrastructure
{
    public struct BytesPerMicrosecond
    {
        public readonly uint value;

        public BytesPerMicrosecond(uint value)
        {
            this.value = value;
        }
    }
}