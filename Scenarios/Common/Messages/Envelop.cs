
using System;
using System.Collections.Generic;
using Transactions.Infrastructure.Network;

namespace Transactions.Scenarios.Common.Messages
{
    public abstract class Envelop<T, U> : IMessage where T : ICloneable where U : Envelop<T, U>, new()
    {
        public string Destination { get; set; }
        public string Source { get; set; }
        public string ID { get; set; }
        public T Data { get; set; }
        public uint Size { get; set; }
        public object Clone()
        {
            return new U()
            {
                Destination = this.Destination,
                Source = this.Source,
                ID = this.ID,
                Data = (T)this.Data.Clone(),
                Size = this.Size
            };
        }
    }
}