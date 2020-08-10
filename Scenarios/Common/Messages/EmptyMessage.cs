
using System;
using System.Collections.Generic;
using Transactions.Infrastructure.Network;

namespace Transactions.Scenarios.Common.Messages
{
    public abstract class EmptyMessage<T> : IMessage
        where T : EmptyMessage<T>, new()
    {
        public virtual string Destination { get; set; }
        public virtual string Source { get; set; }
        public virtual string ID { get; set; }
        public virtual uint Size { get; set; }

        public virtual object Clone()
        {
            return new T()
            {
                Destination = this.Destination,
                Source = this.Source,
                Size = this.Size,
                ID = this.ID
            };
        }
    }
}


