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
using System.Data;
using System.Collections.Generic;
using System.Linq;
using DeltaSnapshot;
using TesterCs.Database;

namespace TesterCs {
    public static class Util {
        public static IEnumerable<Entity> PullDataSet(Int32 dataSetId) {
            Console.WriteLine("called Util.PullDataSet");
            var sourceData = new List<Entity>() {
                    new Entity() { Identifier = "MIN_DATE", LongValue = Int64.MinValue, StringValue = "a", DateTimeOffsetValue = DateTimeOffset.MinValue, BoolValue = false },
                //,   new Entity() { Identifier = "2", LongValue = Int64.MaxValue, StringValue = "abcdefghi", DateTimeOffsetValue = DateTimeOffset.MaxValue, BoolValue = true }
                //,   new Entity() { Identifier = "3" } // all nulls/defaults
                   new Entity() { Identifier = "CUR_MILL", LongValue = 0L, StringValue = String.Empty, DateTimeOffsetValue = DateTimeOffset.Now, BoolValue = false }
                //,   new Entity() { Identifier = "CUR_HOUR", LongValue = 100L, StringValue = "!@#", DateTimeOffsetValue = new DateTimeOffset (DateTimeOffset.Now.Year, DateTimeOffset.Now.Month, DateTimeOffset.Now.Day, DateTimeOffset.Now.Hour, 0, 0, TimeSpan.Zero), BoolValue = true }
            };
            //return sourceData;
            foreach (var e in sourceData) yield return e;
        }

        public static long StartRun(Int32 dataSetId, RunMode runMode) {
            using RunRepository repo = new RunRepository(new UnitOfWork(DatabaseUtil.GetConnection()));
            return repo.Insert(new Run(dataSetId, runMode));
        }

        public static void CompleteRun(Int64 runId, bool isSuccess, string statusMessage, int dataSetCount, int deltaCount) {
            using RunRepository repo = new RunRepository(new UnitOfWork(DatabaseUtil.GetConnection()));
            repo.Update(runId, isSuccess ? "SUCCESS" : "FAILURE", statusMessage, dataSetCount, deltaCount);
        }

        public static bool IsEqual(Entity dt1, Entity dt2) {
            if (dt1 == null || dt2 == null) return false;
            if (ReferenceEquals(dt1, dt2)) return true;

            return (dt1.Identifier == dt2.Identifier
                    && dt1.LongValue == dt2.LongValue
                    && dt1.StringValue == dt2.StringValue
                    && dt1.DateTimeOffsetValue == dt2.DateTimeOffsetValue
                    && dt1.BoolValue == dt2.BoolValue);
        }

        public static void RunGetDeltas() {
            var dataSetId = 88;
            var runId = Util.StartRun(dataSetId, RunMode.SET_DELTA);
            try {
                using var uow = new UnitOfWork(DatabaseUtil.GetConnection());
                using var repoCache = new CacheEntryRepository<Entity>(uow);
                var result = Api.GetDeltas(dataSetId, runId, Util.PullDataSet, EmptyDataSetGetDeltasStrategy.RunSuccessWithBypass, Util.IsEqual,
                    new CacheEntryOperation<Entity>(repoCache.BeginTransaction, repoCache.CommitTransaction, repoCache.RollbackTransaction,
                        repoCache.Insert, repoCache.DeleteDeltaStateLessThanRunId, repoCache.GetLatestById, repoCache.GetRunIdMax, repoCache.GetByRunIdExcludingDeltaState));

                var messages = result.DeltaSnapshots.ToList();
                Console.WriteLine($"Result: " + (result.IsSuccess ? "SUCCESS:" : "FAILURE: ") + result.ErrorMsgs.FirstOrDefault() 
                    + $" RunId: {result.RunId} DataSetCount: {result.DataSetCount} DeltaCount: {result.DeltaCount}");

                Util.CompleteRun(result.RunId, result.IsSuccess, result.IsSuccess ? null : result.ErrorMsgs.FirstOrDefault(), result.DataSetCount, result.DeltaCount);
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
                Util.CompleteRun(runId, false, ex.Message, 0, 0);
            }
        }
    }
    class Program {
        static void Main(string[] args) {
            var testRec = new DeltaSnapshotMessage<Entity>("id", DeltaStateType.ADD, DateTimeOffset.Now, true, null, null);
            var testRec2 = new TestRecord<Entity>(null);
            //int i = 0;

            Util.RunGetDeltas();
            //ResultDeltaSnapshot<Entity> results = DTApi.GetReset(1, Util.TryRunBeginTransaction, Util.GetSourceData, Util.PurgeCache, Util.InsertCacheEntry);
            //var results2 = DTApi.GetFull<DataSet, NetChangeableDataSet>(1, Util.TryRunBeginTransaction, Util.GetSourceData, purgeCache, Util.InsertCacheEntry);
            //(dataSetId: Int32)
            //(tryRunBeginTransaction: TryRunBeginTransactionDelegate)
            //(getSourceData: GetSourceDataDelegate < 'TDataSet>) 
            //(purgeCache: PurgeCacheDelegate < 'TDataSet>)
            //(insertCacheEntry: InsertCacheEntryDelegate < 'TDataSet>) =     
            //Console.WriteLine("Complete");
            //Console.ReadKey();
        }
    }
}
