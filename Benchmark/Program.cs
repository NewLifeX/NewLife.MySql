using Benchmark;

// 命令行参数：--quick 批量方案测试，--driver 三驱动对比测试，无参数运行 BenchmarkDotNet
if (args.Length > 0 && args[0].Equals("--quick", StringComparison.OrdinalIgnoreCase))
{
    await QuickBenchmark.RunAsync(args);
}
else if (args.Length > 0 && args[0].Equals("--driver", StringComparison.OrdinalIgnoreCase))
{
    await DriverBenchmark.RunAsync(args);
}
else
{
    BenchmarkDotNet.Running.BenchmarkRunner.Run<BatchInsertBenchmark>();
}
