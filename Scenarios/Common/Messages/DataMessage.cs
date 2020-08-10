
using System;
using System.Collections.Generic;
using Transactions.Infrastructure.Network;

namespace Transactions.Scenarios.Common.Messages
{
    public class DataMessage<T> : IMessage where T : ICloneable
    {
        public DataMessage(string dest, string source, string id, T data, uint size)
        {
            this.Destination = dest;
            this.Source = source;
            this.ID = id;
            this.Data = data;
            this.Size = size;
        }

        public string Destination { get; private set; }
        public string Source { get; private set; }
        public string ID { get; private set; }
        public T Data { get; private set; }
        public uint Size { get; private set; }
        public object Clone()
        {
            return new DataMessage<T>(
                this.Destination,
                this.Source,
                this.ID,
                (T)this.Data.Clone(),
                this.Size
            );
        }
    }
}