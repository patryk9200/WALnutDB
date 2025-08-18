#nullable enable
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

using WalnutDb.Core;
using WalnutDb.Wal;

namespace WalnutDb.Tests;

file sealed class StressDoc
{
    [DatabaseObjectId] public string Id { get; set; } = "";
    public int Value { get; set; }
}

public sealed class ConcurrentRwStressTests
{
    private static string NewTempDir(string name)
    {
        var dir = Path.Combine(Path.GetTempPath(), "WalnutDbTests", name, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public async Task Concurrent_Writes_Reads_With_Checkpoints_Match_Final_State()
    {
        var dir = NewTempDir("stress-rw");
        await using var wal = new WalWriter(Path.Combine(dir, "wal.log"));
        await using var db = new WalnutDatabase(dir, new DatabaseOptions(), new FileSystemManifestStore(dir), wal);
        var tbl = await db.OpenTableAsync(new TableOptions<StressDoc> { GetId = d => d.Id });

        const int keyCount = 500;
        const int writers = 8;
        const int readers = 4;
        var duration = TimeSpan.FromSeconds(2.5);

        var keys = Enumerable.Range(0, keyCount).Select(i => $"k{i:D4}").ToArray();
        var expected = new ConcurrentDictionary<string, int?>();

        using var cts = new CancellationTokenSource(duration);

        var writerTasks = Enumerable.Range(0, writers).Select(async w =>
        {
            var rnd = new Random(unchecked(Environment.TickCount * 31 + w * 997));
            int opId = 0;

            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var k = keys[rnd.Next(keys.Length)];
                    try
                    {
                        if (rnd.NextDouble() < 0.75) // 75% upsert
                        {
                            int v = Interlocked.Increment(ref opId);
                            var doc = new StressDoc { Id = k, Value = v };
                            var ok = await tbl.UpsertAsync(doc); // ⟵ bez tokena
                            if (ok) expected[k] = v;
                        }
                        else // 25% delete
                        {
                            var ok = await tbl.DeleteAsync(k);  // ⟵ bez tokena
                            if (ok) expected[k] = null;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) { /* swallow */ }
        }).ToArray();

        var readerTasks = Enumerable.Range(0, readers).Select(async r =>
        {
            var rnd = new Random(unchecked(Environment.TickCount * 131 + r * 13));
            int seen = 0;
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        if (rnd.Next(2) == 0)
                        {
                            await foreach (var _ in tbl.GetAllAsync(pageSize: 256, ct: cts.Token))
                                seen++;
                        }
                        else
                        {
                            var k = keys[rnd.Next(keys.Length)];
                            _ = await tbl.GetAsync(k, cts.Token);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) { /* swallow */ }
        }).ToArray();

        var chkTask = Task.Run(async () =>
        {
            while (true)
            {
                try { await Task.Delay(200, cts.Token); }
                catch (OperationCanceledException) { break; }

                try { await db.CheckpointAsync(); } // ⟵ bez tokena
                catch { }
            }
        }, cts.Token);

        await Task.WhenAll(writerTasks.Concat(readerTasks).Append(chkTask));

        await db.CheckpointAsync();

        foreach (var kv in expected)
        {
            var got = await tbl.GetAsync(kv.Key);
            if (kv.Value is null)
            {
                Assert.Null(got);
            }
            else
            {
                Assert.NotNull(got);
                Assert.Equal(kv.Value.Value, got!.Value);
            }
        }
    }
}
