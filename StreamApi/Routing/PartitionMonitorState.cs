namespace StreamApi.Routing;

public enum PartitionMonitorState
{
    Stopped,

    Starting,

    WaitingForConnection,

    Running,

    Reconnecting,

    Unsupported,

    Failed,

    Stopping
}