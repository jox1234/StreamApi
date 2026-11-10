namespace StreamApi.Api.MonitoringToko;

public sealed class MonitoringTokoStoreNotFoundException
    : Exception
{
    public MonitoringTokoStoreNotFoundException(
        string storeCode)
        : base(
            $"Store '{storeCode}' is not registered.")
    {
    }
}