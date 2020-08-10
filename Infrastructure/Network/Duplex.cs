using System.Threading.Tasks;

namespace Transactions.Infrastructure.Network
{
    public class Duplex
    {
        private class Endpoint : IEndpoint
        {
            private ISimplex<IMessage> outgoing;
            private ISimplex<IMessage> ingoing;

            public Endpoint(ISimplex<IMessage> outgoing, ISimplex<IMessage> ingoing)
            {
                this.outgoing = outgoing;
                this.ingoing = ingoing;
            }

            public async Task SendAsync(IMessage message)
            {
                await this.outgoing.SendAsync(message);
            }

            public async Task<IMessage> ReceiveAsync()
            {
                return await this.ingoing.ReceiveAsync();
            }
        }
        
        public IEndpoint Endpoint1 { get; private set; }

        public IEndpoint Endpoint2 { get; private set; }
        
        public Duplex(Orchestrator orchestrator, IOSpec io)
        {
            ISimplex<IMessage> outgoing = new Simplex<IMessage>(orchestrator, orchestrator, io.throughput, io.latency, io.noise);
            ISimplex<IMessage> ingoing = new Simplex<IMessage>(orchestrator, orchestrator, io.throughput, io.latency, io.noise);

            this.Endpoint1 = new Endpoint(outgoing, ingoing);
            this.Endpoint2 = new Endpoint(ingoing, outgoing);
        }
    }
}