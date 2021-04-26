﻿//------------------------------------------------------------------------------
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
        public static IEnumerable<Entity> PullPublisherDataSet(ISubscription subscription) {
            Console.WriteLine("called Util.PullPublisherDataSet");
            var sourceData = new List<Entity>() {
                    new Entity() { Identifier = "MIN_DATE", LongValue = Int64.MinValue, StringValue = "a", DateTimeOffsetValue = DateTimeOffset.MinValue, BoolValue = false },
                    new Entity() { Identifier = "MAX_DATE", LongValue = Int64.MaxValue, StringValue = "abcdefghi", DateTimeOffsetValue = DateTimeOffset.MaxValue, BoolValue = true }
                ,   new Entity() { Identifier = "DEFAULTS" } // all nulls/defaults
                ,   new Entity() { Identifier = "CUR_MILL", LongValue = 0L, StringValue = String.Empty, DateTimeOffsetValue = DateTimeOffset.Now, BoolValue = false }
                ,   new Entity() { Identifier = "CUR_HOUR", LongValue = 100L, StringValue = "!@#", DateTimeOffsetValue = new DateTimeOffset (DateTimeOffset.Now.Year, DateTimeOffset.Now.Month, DateTimeOffset.Now.Day, DateTimeOffset.Now.Hour, 0, 0, TimeSpan.Zero), BoolValue = true }
            };
            foreach (var e in sourceData) yield return e;
        }

        public static void RunGetDeltas() {
            var subscription = new Subscription(88, String.Empty);
            var runId = RunService.StartRun(subscription.SubscriptionDataSetId, RunModeType.SET_DELTA);
            try {
                using var uow = new UnitOfWork(DatabaseUtil.GetConnection());
                using var repoCache = new CacheEntryRepository<Entity>(uow);
                var result = Api.Subscriber.GetDeltas(subscription, runId, Util.PullPublisherDataSet, EmptyDataSetGetDeltasStrategyType.RunSuccessWithBypass, Entity.IsEqual,
                    new CacheEntryOperation<Entity>(repoCache.BeginTransaction, repoCache.CommitTransaction, repoCache.RollbackTransaction,
                        repoCache.Insert, repoCache.DeleteDeltaStateLessThanRunId, repoCache.GetLatestById, repoCache.GetRunIdMax, repoCache.GetByRunIdExcludingDeltaState));

                var messages = result.DeltaSnapshots.ToList();
                Console.WriteLine((result.IsSuccess ? "SUCCESS" : "FAILURE") + " " + result.ErrorMsgs.FirstOrDefault() 
                    + $" RunId:{result.RunId} DataSetCount:{result.DataSetCount} DeltaCount:{result.DeltaCount}");

                RunService.CompleteRun(result.RunId, result.IsSuccess, result.IsSuccess ? null : result.ErrorMsgs.FirstOrDefault(), result.DataSetCount, result.DeltaCount);
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
                RunService.CompleteRun(runId, false, ex.Message, 0, 0);
            }
        }
    }
    class Program {
        static void Main(string[] args) {
            var testRec = new DeltaSnapshotMessage<Entity>("id", DeltaStateType.ADD, DateTimeOffset.Now, true, null, null);
            var testRec2 = new TestRecord<Entity>(null);

            Util.RunGetDeltas();
        }
    }
}
