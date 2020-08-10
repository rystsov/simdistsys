using System.Threading.Tasks;

namespace Transactions.Infrastructure.Network
{
    public interface IEndpoint
    {
        Task SendAsync(IMessage message);
        Task<IMessage> ReceiveAsync();
    }
}