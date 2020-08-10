using System;

namespace Transactions.Infrastructure
{
    public struct Microsecond
    {
        public readonly ulong value;

        public Microsecond(ulong value)
        {
            this.value = value;
        }

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            
            return obj != null && obj is Microsecond? ((Microsecond)obj).value == this.value : false;
        }

        public override int GetHashCode()
        {
            return this.value.GetHashCode();
        }

        public static bool operator ==(Microsecond a, Microsecond b)
        {
            return a.Equals(b);
        }

        public static bool operator <(Microsecond a, Microsecond b)
        {
            return a.value < b.value;
        }

        public static bool operator >(Microsecond a, Microsecond b)
        {
            return a.value > b.value;
        }
        
        public static bool operator !=(Microsecond a, Microsecond b)
        {
            return !a.Equals(b);
        }
    }
}