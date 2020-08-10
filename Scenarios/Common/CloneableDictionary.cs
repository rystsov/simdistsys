using System;
using System.Linq;
using System.Collections.Generic;

namespace Transactions.Scenarios.Common
{
    public class CloneableDictionary<K,V> : ICloneable
    {
        public readonly Dictionary<K, V> data;

        public CloneableDictionary(Dictionary<K, V> data)
        {
            this.data = data;
        }

        public object Clone()
        {
            return new CloneableDictionary<K,V>(this.data.ToDictionary(x => x.Key, y =>
            {
                var value = y.Value;
                if (value == null)
                {
                    return value;
                }
                else if (value is ICloneable ic)
                {
                    return (V)ic.Clone();
                }
                else
                {
                    return value;
                }
            }));
        }
    }
}