using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Diagnosers;

var cfg = ManualConfig.Create(DefaultConfig.Instance)
    .AddJob(Job.Default.WithId(".NET 8"))
    .AddDiagnoser(MemoryDiagnoser.Default);

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, cfg);
