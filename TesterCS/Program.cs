//------------------------------------------------------------------------------
//    DeltaSnapshot.CLR
//    Copyright(C) 2021 Clay Lipscomb
//
//    This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.
//
//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU General Public License for more details.
//
//    You should have received a copy of the GNU General Public License
//    along with this program. If not, see<http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using DeltaSnapshot;
using TesterCache;

namespace TesterCs {
    public static class Util {
        public static IEnumerable<TesterEntity> PullPublisherDataSet(ISubscription subscription) {
            Console.WriteLine("CONSUMER: called Util.PullPublisherDataSet");
            var sourceData = new List<TesterEntity>() {
                  new TesterEntity() { Identifier = "CUR_MINU", LongValue = 100L, StringValue = "&*(", DateTimeOffsetValue = new DateTimeOffset (DateTimeOffset.Now.Year, DateTimeOffset.Now.Month, DateTimeOffset.Now.Day, DateTimeOffset.Now.Hour, DateTimeOffset.Now.Minute, 0, TimeSpan.Zero), BoolValue = true }
                , new TesterEntity() { Identifier = "CUR_HOUR", LongValue = 100L, StringValue = "!@#", DateTimeOffsetValue = new DateTimeOffset (DateTimeOffset.Now.Year, DateTimeOffset.Now.Month, DateTimeOffset.Now.Day, DateTimeOffset.Now.Hour, 0, 0, TimeSpan.Zero), BoolValue = true }
                , new TesterEntity() { Identifier = "CUR_DATE", LongValue = 100L, StringValue = "!@#", DateTimeOffsetValue = new DateTimeOffset (DateTimeOffset.Now.Year, DateTimeOffset.Now.Month, DateTimeOffset.Now.Day, 0, 0, 0, TimeSpan.Zero), BoolValue = true }
                //, new TesterEntity() { Identifier = "MIN_LONG", LongValue = Int64.MinValue, StringValue = "a", DateTimeOffsetValue = new DateTimeOffset (2021, 1, 1, 0, 0, 0, TimeSpan.Zero), BoolValue = false }
                //, new TesterEntity() { Identifier = "MAX_LONG", LongValue = Int64.MaxValue, StringValue = "abcdefghi", DateTimeOffsetValue = new DateTimeOffset (2021, 1, 1, 0, 0, 0, TimeSpan.Zero), BoolValue = true }
                //, new TesterEntity() { Identifier = "DEFAULTS" } // all nulls/defaults
                //, new TesterEntity() { Identifier = "CUR_MILL", LongValue = 0L, StringValue = String.Empty, DateTimeOffsetValue = DateTimeOffset.Now, BoolValue = false }
            };
            foreach (var e in sourceData) yield return e;
        }
        public static void RunTest() {
            var subscription = new Subscription(88, String.Empty);
            try {
                using var uow = new UnitOfWork(DatabaseUtil.GetConnection());
                using var repoCache = new CacheEntryRepository<TesterEntity>(uow);
                //var result = repoCache.GetLatestByIdTest<Int64?, TesterEntity>(subscription.SubscriptionDataSetId, "CUR_MILL");
            } catch (Exception ex) {
                Console.WriteLine($"CONSUMER: {ex.Message}");
            }
        }    

        public static void RunGetDeltasBatch() {
            var subscription = new Subscription(88, String.Empty);
            var runId = RunService.StartRun(subscription.SubscriptionDataSetId, "SET_DELTA");
            try {
                RunResultType<TesterEntity> result;
                using (var uow = new UnitOfWork(DatabaseUtil.GetConnection())) {
                    using var repoCache = new CacheEntryRepository<TesterEntity>(uow);
                    result = Api.Subscriber.GetDeltasBatch(
                        subscription,
                        runId,
                        Util.PullPublisherDataSet,
                        EmptyPublisherDataSetGetDeltasStrategyType.DefaultProcessingDeleteAll,
                        TesterEntity.IsEqual,
                        new CacheEntryOperationBatch<long, TesterEntity>(
                            repoCache.BeginTransaction,
                            repoCache.CommitTransaction,
                            repoCache.RollbackTransaction,

                            repoCache.GetRunIdMaxOfDataSet,
                            repoCache.GetLatestById<long, TesterEntity>,
                            repoCache.Insert<long, TesterEntity>,
                            repoCache.Update<long, TesterEntity>,
                            repoCache.GetDataSetRunExcludingDeltaState<long, TesterEntity>));

                    var messages = result.DeltaSnapshots.ToList();
                }
                Console.WriteLine("CONSUMER: " + (result.IsSuccess ? "SUCCESS" : "FAILURE") + " " + result.ErrorMsgs.FirstOrDefault()
                    + $" RunId:{result.RunId} DataSetCount:{result.DataSetCount} DeltaCount:{result.DeltaCount}");

                RunService.CompleteRun(result.RunId, result.IsSuccess, result.IsSuccess ? null : result.ErrorMsgs.FirstOrDefault(), result.DataSetCount, result.DeltaCount);
            } catch (Exception ex) {
                Console.WriteLine($"CONSUMER: {ex.Message}");
                RunService.CompleteRun(runId, false, ex.Message, 0, 0);
            }
        }
    }
    class Program {
        static void Main(string[] args) {
            //var testRec = new DeltaSnapshotMessage<TesterEntity>("id", DeltaStateType.ADD, DateTimeOffset.Now, true, null, null);
            //var testRec2 = new TestRecord<Entity>(null);

            Util.RunGetDeltasBatch();
            //Util.RunTest();
        }
    }
}
