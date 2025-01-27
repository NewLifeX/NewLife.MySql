using System.ComponentModel;
using System.Diagnostics;

namespace NewLife.MySql.Common;

class ConnectAttributes
{
    [DisplayName("_client_name")]
    public String ClientName => "NewLife.MySql";

    [DisplayName("_pid")]
    public String PID => Process.GetCurrentProcess().Id.ToString();

    [DisplayName("_client_version")]
    public String ClientVersion => GetType().Assembly.GetName().Version.ToString();

    [DisplayName("_os")]
    public String OS { get; } = Environment.OSVersion.Platform + "";

    [DisplayName("_os_details")]
    public String OSDetails { get; } = Environment.MachineName;

    private Boolean Is64BitOS() => Environment.GetEnvironmentVariable("PROCESSOR_ARCHITECTURE") == "AMD64";
}