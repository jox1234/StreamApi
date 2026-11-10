namespace StreamApi.Api.MonitoringToko;

public sealed class MonitoringTokoBranchNotFoundException
    : Exception
{
    public MonitoringTokoBranchNotFoundException(
        string storeCode)
        : base(
            $"Branch code for store '{storeCode}' was not found.")
    {
    }
}