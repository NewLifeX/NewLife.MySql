using Benchmark;

// 命令行参数 --quick 运行快速测试，否则运行 BenchmarkDotNet 完整测试
if (args.Length > 0 && args[0].Equals("--quick", StringComparison.OrdinalIgnoreCase))
{
    await QuickBenchmark.RunAsync(args);
}
else
{
    BenchmarkDotNet.Running.BenchmarkRunner.Run<BatchInsertBenchmark>();
}
