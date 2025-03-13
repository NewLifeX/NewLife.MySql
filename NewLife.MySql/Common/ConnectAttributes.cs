using System.ComponentModel;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace NewLife.MySql.Common;

class ConnectAttributes
{
    static String _client;
    static String _version;
    static String _os;
    static String _platform;
    static String _framework;

    static ConnectAttributes()
    {
        var asm = typeof(ConnectAttributes).Assembly;
        _client = asm.GetName().Name;
        _version = asm.GetName().Version.ToString();

        var mi = MachineInfo.GetCurrent();
        //_os = Environment.OSVersion.Platform + "";
        _os = mi.OSName!;

#if NETCOREAPP || NETSTANDARD
        _platform = RuntimeInformation.ProcessArchitecture.ToString().ToLower();
#else
        _platform = IntPtr.Size == 8 ? "x64" : "x86";
#endif

        _framework = "";
        asm = Assembly.GetEntryAssembly();
        if (asm != null)
        {
            var tar = asm.GetCustomAttribute<TargetFrameworkAttribute>();
            if (tar != null) _framework = !tar.FrameworkDisplayName.IsNullOrEmpty() ? tar.FrameworkDisplayName : tar.FrameworkName;
        }
    }

    [DisplayName("_client_name")]
    public String ClientName => _client;

    [DisplayName("_client_version")]
    public String ClientVersion => _version;

    [DisplayName("_client_licence")]
    public String ClientLicence => "MIT";

    [DisplayName("_pid")]
    public String PID => Process.GetCurrentProcess().Id.ToString();

    [DisplayName("_os")]
    public String OS => _os;

    [DisplayName("_platform")]
    public String Platform => _platform;

    [DisplayName("_framework")]
    public String Framework => _framework;
}