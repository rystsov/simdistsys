using System;
using System.Linq;
using System.Collections.Generic;

namespace Transactions.Scenarios.Common
{
    public class CloneableLong : ICloneable
    {
        public readonly long data;

        public CloneableLong(long data)
        {
            this.data = data;
        }

        public object Clone()
        {
            return new CloneableLong(this.data);
        }
    }
}