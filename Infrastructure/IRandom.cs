namespace Transactions.Infrastructure
{
    public interface IRandom
    {
        int Next(int max);
        int Next();
        double NextDouble();
    }
}