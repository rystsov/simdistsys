using System;
using System.Linq;
using System.Collections.Generic;

namespace Transactions.Scenarios.Common
{
    public class CloneableT<T> : ICloneable
    {
        public readonly T data;

        public CloneableT(T data)
        {
            this.data = data;
        }

        public object Clone()
        {
            return new CloneableT<T>(this.data);
        }
    }
}