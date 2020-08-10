using System;
using System.Collections.Generic;

namespace Transactions.Infrastructure
{
    public class MinHeap<T>
    {
        private class Node
        {
            public T value;
            public ulong priority;
        }

        private List<Node> array = new List<Node>();
        private Dictionary<T, int> index = new Dictionary<T, int>();

        public ulong Min()
        {
            if (this.Count == 0) throw new Exception();

            return array[0].priority;
        }

        public void Push(T value, ulong priority)
        {
            if (this.index.ContainsKey(value))
            {
                throw new Exception($"${nameof(MinHeap<T>)} can only have unique values");
            }

            this.array.Add(new Node
            {
                value = value,
                priority = priority
            });

            this.index.Add(value, this.array.Count - 1);

            // pop up

            this.BubbleUp(this.array.Count - 1);

            // 0 -> {1, 2} // n -> { 2n+1, 2n+2 } ; m -> (m-1)/2
            // 1 -> {3, 4}
        }

        public T Pop()
        {
            if (array.Count == 0) throw new Exception();

            var value = array[0].value;
            this.Delete(value);
            return value;
        }
    
        public bool Delete(T value)
        {
            if (!index.ContainsKey(value))
            {
                return false;
            }

            var i = this.index[value];
            var iPP = this.array[i].priority;
            var iCP = this.array[this.array.Count - 1].priority;

            Swap(i, array.Count - 1);

            this.index.Remove(value);
            array.RemoveAt(array.Count - 1);

            if (iCP < iPP)
            {
                BubbleUp(i);
            }
            else if (iCP > iPP)
            {
                BubbleDown(i);
            }

            return true;
        }

        public int Count => this.array.Count;

        private void Swap(int a, int b)
        {
            var tmp1 = this.array[a].priority;  
            var tmp2 = this.array[a].value;
            this.index[this.array[a].value] = b;
            this.index[this.array[b].value] = a;
            this.array[a].priority = this.array[b].priority;
            this.array[a].value = this.array[b].value;
            this.array[b].priority = tmp1;
            this.array[b].value = tmp2;
        }

        private void BubbleUp(int i)
        {
            while (i != 0)
            {
                var p = (i-1)/2;
                if (array[p].priority > array[i].priority)
                {
                    Swap(p, i);
                    i = p;
                }
                else
                {
                    break;
                }
            }
        }

        private void BubbleDown(int i)
        {
            var left = 2*i+1;
            var right = 2*i+2;

            while (right < array.Count)
            {
                if (array[i].priority <= Math.Min(array[left].priority, array[right].priority))
                {
                    return;
                }

                if (array[left].priority < array[right].priority)
                {
                    Swap(i, left);
                    i = left;
                }
                else
                {
                    Swap(i, right);
                    i = right;
                }

                left = 2*i+1;
                right = 2*i+2;
            }

            if (left < array.Count && array[i].priority > array[left].priority)
            {
                Swap(i, left);
            }
        }
    }
}