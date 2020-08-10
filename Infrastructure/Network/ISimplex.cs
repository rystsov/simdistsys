using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Transactions.Infrastructure.Network
{
    public interface ISimplex<T> where T : IMessage
    {
        Task SendAsync(T message);
        Task<T> ReceiveAsync();
    }
}