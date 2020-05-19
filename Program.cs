using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;

namespace RavenDBPessimistic
{
    class Program
    {
        private const int DelayTime = 10;
        private const int ConcurrentWrites = 50;

        static async Task Main(string[] args)
        {
            var store = new DocumentStore
            {
                Urls = new[] {"http://localhost:8080"}, Database = "PessimisticTryouts"
            };

            store.Initialize();

            for (int i = 0; i < 5; i++)
            {
                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new Document
                {
                    Id = "optimistic/1"
                });
                await session.StoreAsync(new Document
                {
                    Id = "pessimistic/1"
                });
                await session.SaveChangesAsync();

                Optimistic(store);
                Pessimistic(store);
            }

            Console.ReadLine();
        }

        private static void Pessimistic(IDocumentStore store)
        {
            Console.WriteLine("Pessimistic");

            var countDownEvent = new CountdownEvent(ConcurrentWrites);

            var stopWatch = Stopwatch.StartNew();
            for (var i = 0; i < ConcurrentWrites; i++)
            {
                _ = RetryModifyPessimisticWithDelay(store, countDownEvent, i);
                // if (i == 5)
                // {
                //     Environment.Exit(0);
                // }
            }

            countDownEvent.Wait();

            stopWatch.Stop();

            Console.WriteLine("Duration");
            Console.WriteLine(stopWatch.Elapsed.TotalMilliseconds);
        }

        private static void Optimistic(IDocumentStore store)
        {
            Console.WriteLine("Optimistic");

            var countDownEvent = new CountdownEvent(ConcurrentWrites);

            var stopWatch = Stopwatch.StartNew();
            for (var i = 0; i < ConcurrentWrites; i++)
            {
                _ = RetryModifyOptimisticWithDelay(store, countDownEvent, i);
            }

            countDownEvent.Wait();

            stopWatch.Stop();

            Console.WriteLine("Duration");
            Console.WriteLine(stopWatch.Elapsed.TotalMilliseconds);
            Console.WriteLine("");
        }

        private static async Task ModifyOptimistic(IDocumentStore store, long counter)
        {
            using var session = store.OpenAsyncSession();
            session.Advanced.UseOptimisticConcurrency = true;
            var document = await session.LoadAsync<Document>("optimistic/1");

            document.Items.Add(counter.ToString());

            await session.SaveChangesAsync();
        }

        private static async Task RetryModifyOptimisticWithDelay(IDocumentStore store, CountdownEvent @event, long counter)
        {
            do
            {
                try
                {
                    await ModifyOptimistic(store, counter);
                    @event.Signal();
                    return;
                }
                catch (Exception e)
                {
                    Console.Write($"!{counter}");
                    await Task.Delay(DelayTime);
                }
            } while (true);
        }

        private static async Task ModifyPessimistic(IDocumentStore store, long counter)
        {
            var reservationIndex = await LockResource(store, "pessimistic/1", TimeSpan.FromMinutes(1));

            try
            {
                using var session = store.OpenAsyncSession();
                var document = await session.LoadAsync<Document>("pessimistic/1");

                document.Items.Add(counter.ToString());

                await session.SaveChangesAsync();
            }
            finally
            {
                await ReleaseResource(store, "pessimistic/1", reservationIndex);
            }
        }

        private static async Task RetryModifyPessimisticWithDelay(IDocumentStore store, CountdownEvent @event, long counter)
        {
            do
            {
                try
                {
                    await ModifyPessimistic(store, counter);
                    @event.Signal();
                    return;
                }
                catch (Exception)
                {
                    Console.Write($"!{counter}");
                    await Task.Delay(DelayTime);
                }
            } while (true);
        }

        private class Lock
        {
            public DateTime? ReservedUntil { get; set; }
        }

        private static async Task<long> LockResource(IDocumentStore store, string resourceName, TimeSpan duration)
        {
            while (true)
            {
                var now = DateTime.UtcNow;

                var resource = new Lock
                {
                    ReservedUntil = now.Add(duration)
                };

                var saveResult = await store.Operations.SendAsync(
                    new PutCompareExchangeValueOperation<Lock>(resourceName, resource, 0));

                if (saveResult.Successful)
                {
                    // resourceName wasn't present - we managed to reserve
                    return saveResult.Index;
                }

                // At this point, Put operation failed - someone else owns the lock or lock time expired
                if (saveResult.Value.ReservedUntil < now)
                {
                    // Time expired - Update the existing key with the new value
                    var takeLockWithTimeoutResult = await store.Operations.SendAsync(
                        new PutCompareExchangeValueOperation<Lock>(resourceName, resource, saveResult.Index));

                    if (takeLockWithTimeoutResult.Successful)
                    {
                        return takeLockWithTimeoutResult.Index;
                    }
                }

                await Task.Delay(50);
            }
        }

        private static  async Task ReleaseResource(IDocumentStore store, string resourceName, long index)
        {
            var deleteResult
                = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<Lock>(resourceName, index));

            if (deleteResult.Successful)
            {
                return;
            }

            throw new TimeoutException();

            // We have 2 options here:
            // deleteResult.Successful is true - we managed to release resource
            // deleteResult.Successful is false - someone else took the lock due to timeout
        }
    }

    class Document
    {
        public string Id { get; set; }
        public List<string> Items { get; set; } = new List<string>();
    }
}