using System;
using System.Collections.Generic;

namespace Transactions.Infrastructure.Network
{
    public interface IMessage : ICloneable
    {
        string ID { get; }
        string Destination { get; }
        string Source { get; }
        uint Size { get; }
    }
}