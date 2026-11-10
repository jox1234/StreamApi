using System.Diagnostics;
using System.Runtime.InteropServices;

namespace StreamApi.Performance;

public sealed class ResourceMonitor
{
    private readonly Process _process;

    private TimeSpan _lastProcessCpuTime;
    private DateTimeOffset _lastSampleTime;

    private CpuSample? _lastSystemCpuSample;

    public ResourceMonitor()
    {
        _process = Process.GetCurrentProcess();

        _lastProcessCpuTime =
            _process.TotalProcessorTime;

        _lastSampleTime =
            DateTimeOffset.UtcNow;

        _lastSystemCpuSample =
            ReadSystemCpuSample();
    }

    public ResourceSnapshot Capture()
    {
        _process.Refresh();

        var now =
            DateTimeOffset.UtcNow;

        var processCpuTime =
            _process.TotalProcessorTime;

        var elapsed =
            now - _lastSampleTime;

        var processCpuPercent = 0.0;

        if (elapsed.TotalMilliseconds > 0)
        {
            var cpuTimeDelta =
                (processCpuTime -
                 _lastProcessCpuTime).TotalMilliseconds;

            processCpuPercent =
                cpuTimeDelta /
                (elapsed.TotalMilliseconds *
                 Environment.ProcessorCount) *
                100.0;
        }

        _lastProcessCpuTime =
            processCpuTime;

        _lastSampleTime =
            now;

        var systemCpuPercent =
            CaptureSystemCpuPercent();

        var memory =
            CaptureMemory();

        return new ResourceSnapshot
        {
            ProcessorCount =
                Environment.ProcessorCount,

            SystemCpuPercent =
                Math.Clamp(
                    systemCpuPercent,
                    0,
                    100),

            ProcessCpuPercent =
                Math.Clamp(
                    processCpuPercent,
                    0,
                    100),

            TotalMemoryBytes =
                memory.Total,

            AvailableMemoryBytes =
                memory.Available,

            ProcessMemoryBytes =
                _process.WorkingSet64
        };
    }

    private double CaptureSystemCpuPercent()
    {
        var current =
            ReadSystemCpuSample();

        if (_lastSystemCpuSample is null ||
            current is null)
        {
            _lastSystemCpuSample =
                current;

            return 0;
        }

        var previous =
            _lastSystemCpuSample.Value;

        _lastSystemCpuSample =
            current;

        var totalDelta =
            current.Value.Total -
            previous.Total;

        var idleDelta =
            current.Value.Idle -
            previous.Idle;

        if (totalDelta <= 0)
        {
            return 0;
        }

        return
            (1.0 -
             idleDelta /
             (double)totalDelta) *
            100.0;
    }

    private static CpuSample? ReadSystemCpuSample()
    {
        if (OperatingSystem.IsWindows())
        {
            return ReadWindowsCpuSample();
        }

        if (OperatingSystem.IsLinux())
        {
            return ReadLinuxCpuSample();
        }

        return null;
    }

    private static CpuSample? ReadLinuxCpuSample()
    {
        try
        {
            var line =
                File.ReadLines("/proc/stat")
                    .FirstOrDefault(
                        x => x.StartsWith(
                            "cpu ",
                            StringComparison.Ordinal));

            if (line is null)
            {
                return null;
            }

            var parts =
                line.Split(
                    ' ',
                    StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length < 5)
            {
                return null;
            }

            var user =
                ulong.Parse(parts[1]);

            var nice =
                ulong.Parse(parts[2]);

            var system =
                ulong.Parse(parts[3]);

            var idle =
                ulong.Parse(parts[4]);

            var iowait =
                parts.Length > 5
                    ? ulong.Parse(parts[5])
                    : 0;

            var irq =
                parts.Length > 6
                    ? ulong.Parse(parts[6])
                    : 0;

            var softirq =
                parts.Length > 7
                    ? ulong.Parse(parts[7])
                    : 0;

            var steal =
                parts.Length > 8
                    ? ulong.Parse(parts[8])
                    : 0;

            var total =
                user +
                nice +
                system +
                idle +
                iowait +
                irq +
                softirq +
                steal;

            var idleTotal =
                idle +
                iowait;

            return new CpuSample(
                total,
                idleTotal);
        }
        catch
        {
            return null;
        }
    }

    private static CpuSample? ReadWindowsCpuSample()
    {
        if (!GetSystemTimes(
                out var idle,
                out var kernel,
                out var user))
        {
            return null;
        }

        var idleValue =
            FileTimeToUInt64(idle);

        var kernelValue =
            FileTimeToUInt64(kernel);

        var userValue =
            FileTimeToUInt64(user);

        return new CpuSample(
            kernelValue + userValue,
            idleValue);
    }

    private static (long Total, long Available) CaptureMemory()
    {
        if (OperatingSystem.IsWindows())
        {
            if (GetMemoryStatus(
                    out var status))
            {
                return (
                    (long)status.ullTotalPhys,
                    (long)status.ullAvailPhys);
            }
        }

        if (OperatingSystem.IsLinux())
        {
            try
            {
                var lines =
                    File.ReadAllLines(
                        "/proc/meminfo");

                long totalKb = 0;
                long availableKb = 0;

                foreach (var line in lines)
                {
                    if (line.StartsWith(
                            "MemTotal:",
                            StringComparison.Ordinal))
                    {
                        totalKb =
                            ParseMemInfoValue(line);
                    }
                    else if (line.StartsWith(
                                 "MemAvailable:",
                                 StringComparison.Ordinal))
                    {
                        availableKb =
                            ParseMemInfoValue(line);
                    }
                }

                return (
                    totalKb * 1024,
                    availableKb * 1024);
            }
            catch
            {
                // Fall through.
            }
        }

        var gcInfo =
            GC.GetGCMemoryInfo();

        return (
            gcInfo.TotalAvailableMemoryBytes,
            Math.Max(
                0,
                gcInfo.TotalAvailableMemoryBytes -
                GC.GetTotalMemory(false)));
    }

    private static long ParseMemInfoValue(
        string line)
    {
        var parts =
            line.Split(
                ' ',
                StringSplitOptions.RemoveEmptyEntries);

        return long.Parse(parts[1]);
    }

    private static ulong FileTimeToUInt64(
    System.Runtime.InteropServices.ComTypes.FILETIME value)
    {
        return
            ((ulong)(uint)value.dwHighDateTime << 32) |
            (uint)value.dwLowDateTime;
    }

    private readonly record struct CpuSample(
        ulong Total,
        ulong Idle);

    [DllImport(
        "kernel32.dll",
        SetLastError = true)]
    private static extern bool GetSystemTimes(
        out System.Runtime.InteropServices.ComTypes.FILETIME lpIdleTime,
        out System.Runtime.InteropServices.ComTypes.FILETIME lpKernelTime,
        out System.Runtime.InteropServices.ComTypes.FILETIME lpUserTime);

    [StructLayout(LayoutKind.Sequential)]
    private struct MEMORYSTATUSEX
    {
        public uint dwLength;
        public uint dwMemoryLoad;
        public ulong ullTotalPhys;
        public ulong ullAvailPhys;
        public ulong ullTotalPageFile;
        public ulong ullAvailPageFile;
        public ulong ullTotalVirtual;
        public ulong ullAvailVirtual;
        public ulong ullAvailExtendedVirtual;
    }

    [DllImport(
        "kernel32.dll",
        SetLastError = true)]
    private static extern bool GlobalMemoryStatusEx(
        ref MEMORYSTATUSEX lpBuffer);

    private static bool GetMemoryStatus(
        out MEMORYSTATUSEX status)
    {
        status = new MEMORYSTATUSEX
        {
            dwLength =
                (uint)Marshal.SizeOf<MEMORYSTATUSEX>()
        };

        return GlobalMemoryStatusEx(
            ref status);
    }

    public async Task<ResourceSnapshot> CaptureBaselineAsync(
    CancellationToken cancellationToken)
    {
        /*
         * Give the operating system a short interval
         * so CPU utilization can be measured meaningfully.
         */
        await Task.Delay(
            TimeSpan.FromSeconds(1),
            cancellationToken);

        return Capture();
    }
}