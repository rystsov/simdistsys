
using System;
using System.Collections.Generic;
using Transactions.Infrastructure.Network;

namespace Transactions.Scenarios.Common.Messages
{
    public abstract class Letter<T> : IMessage where T : Letter<T>, new()
    {
        public string Destination { get; set; }
        public string Source { get; set; }
        public string ID { get; set; }
        public uint Size { get; set; }
        public virtual object Clone()
        {
            return new T()
            {
                Destination = this.Destination,
                Source = this.Source,
                ID = this.ID,
                Size = this.Size
            };
        }
    }
}