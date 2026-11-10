namespace StreamApi.Api.MonitoringToko;

public sealed class MonitoringTokoBranchMismatchException
    : Exception
{
    public MonitoringTokoBranchMismatchException(
        string storeCode,
        string requestedBranchCode,
        string actualBranchCode)
        : base(
            $"Branch code mismatch for store '{storeCode}'. " +
            $"Requested '{requestedBranchCode}', " +
            $"actual '{actualBranchCode}'.")
    {
    }
}