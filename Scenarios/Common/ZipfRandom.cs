using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Transactions.Infrastructure;

namespace Transactions.Scenarios.Common
{
    // http://www.uvm.edu/pdodds/research/papers/others/2001/axtell2001a.pdf
    // https://medium.com/@jasoncrease/zipf-54912d5651cc
    public class ZipfRandom
    {
        readonly double[] cdf;
        readonly IRandom random;
        
        public ZipfRandom(IRandom random, double skew, int n)
        {
            this.cdf = new double[n];
            this.random = random;
            for (int i=0;i<n;i++)
            {
                this.cdf[i] = ZipfCdfApprox(i+1, skew, n);
            }
        }

        public int RandomRank()
        {
            var p = random.NextDouble();
            return BiSearch(cdf, p, 0, cdf.Length - 1);
        }

        private static double ZipfCdfApprox(double k, double s, double N) {
            if (k > N || k < 1)
                throw new ArgumentException("k must be between 1 and N");

            double a = (Math.Pow(k, 1 - s) - 1) / (1 - s) + 0.5 + Math.Pow(k, -s) / 2 + s / 12 - Math.Pow(k, -1 - s) * s / 12;
            double b = (Math.Pow(N, 1 - s) - 1) / (1 - s) + 0.5 + Math.Pow(N, -s) / 2 + s / 12 - Math.Pow(N, -1 - s) * s / 12;

            return a / b;
        }
        
        private static int BiSearch(double[] cdf, double p, int first, int last)
        {
            if (first == last) return first;
            
            int mid = (first + last) / 2;
            if (p <= cdf[mid])
            {
                if (mid == first)
                {
                    return first;
                }
                else
                {
                    return BiSearch(cdf, p, first, mid);
                }
            }
            else
            {
                if (mid == first)
                {
                    return last;
                }
                else
                {
                    return BiSearch(cdf, p, mid, last);
                }
            }
        }
    }
}