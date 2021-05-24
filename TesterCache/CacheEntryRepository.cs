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
using Dapper;
using System.Data;
using Newtonsoft.Json;
using DeltaSnapshot;

namespace TesterCache {
    public class CacheEntryRepository<T> : IDisposable where T : class, IDataSetEntity, new() {
        private class JsonTypeHandler : SqlMapper.ITypeHandler {
            public void SetValue(IDbDataParameter parameter, object value) { parameter.Value = JsonConvert.SerializeObject(value); }
            public object Parse(Type destinationType, object value) => JsonConvert.DeserializeObject(value as string, destinationType);
        }
        private void InitializeDapper() {
            SqlMapper.AddTypeHandler(typeof(T), new JsonTypeHandler());
        }

        protected IUnitOfWork unitOfWork;
        public CacheEntryRepository(IUnitOfWork unitOfWork) {
            this.unitOfWork = unitOfWork;
            InitializeDapper();
        }

        public void Dispose() { unitOfWork.Dispose(); }
        private const string baseFromSql = @" FROM dlta_cache_snapshot cs ";
        private readonly string baseSelectFromSql = @"SELECT    cs.cache_snapshot_id AS CacheSnapshotId,
                                                                cs.subscription_data_set_id AS SubscriptionDataSetId,
                                                                cs.run_id AS RunId,
                                                                cs.entity_identifier AS EntityIdentifier,
                                                                cs.entity_delta_code AS EntityDeltaCode,
                                                                cs.entity_delta_date AS EntityDeltaDate,
                                                                cs.entity_data_current AS EntityDataCurrent,
                                                                cs.entity_data_previous AS EntityDataPrevious "
                                                    + baseFromSql;
        //private readonly string baseSelectFromSqlNew = @"SELECT CAST(cs.subscription_data_set_id AS NUMBER(38)) AS SubscriptionDataSetId,
        //                                                        CAST(cs.run_id AS NUMBER(38)) AS RunId,
        //                                                        cs.entity_identifier AS EntityIdentifier,
        //                                                        cs.entity_delta_code AS EntityDeltaCode,
        //                                                        cs.entity_delta_date AS EntityDeltaDate,
        //                                                        cs.entity_data_current AS EntityDataCurrent,
        //                                                        cs.entity_data_previous AS EntityDataPrevious "
        //                                            + baseFromSql;
        public void BeginTransaction() {
            Console.WriteLine($"CONSUMER: called BeginTransaction()");
            unitOfWork.Begin(); 
        }
        public void CommitTransaction() {
            Console.WriteLine($"CONSUMER: called CommitTransaction()");
            unitOfWork.Commit(); 
        }
        public void RollbackTransaction() {
            Console.WriteLine($"CONSUMER: called RollbackTransaction()");
            unitOfWork.Rollback(); 
        }

        public IEnumerable<DeltaSnapshotCacheRowType<long, TEntity>> GetDataSetRunExcludingDeltaState<TCachePrimaryKey, TEntity>(Int64 subscriptionDataSetId, long runId, string deltaStateCodeExclude) 
                where TEntity : class, IDataSetEntity, new() {
            Console.WriteLine($"CONSUMER: called CacheEntryRepository.GetByRunIdExcludingDeltaState {deltaStateCodeExclude}");
            string query = baseSelectFromSql + @" WHERE cs.subscription_data_set_id = :subscriptionDataSetId AND cs.run_id = :runId AND cs.entity_delta_code != :deltaStateCodeExclude ";
            var recordResult = unitOfWork.Connection.Query<CacheEntry<TEntity>>(query, new { subscriptionDataSetId, runId, deltaStateCodeExclude })
                .Select(r => new DeltaSnapshotCacheRowType<long, TEntity>(r.CacheSnapshotId, r.SubscriptionDataSetId, r.RunId, r.EntityIdentifier, r.EntityDeltaCode, r.EntityDeltaDate, r.EntityDataCurrent, r.EntityDataPrevious));
            return recordResult;
        }
        //public IEnumerable<DeltaSnapshotCacheRowType<long, TEntity>> GetDataSetRun<TCachePrimaryKey, TEntity>(int subscriptionDataSetId, long runId)
        //        where TEntity : class, IDataSetEntity, new() {
        //    Console.WriteLine($"CONSUMER: called CacheEntryRepository.GetByRunId");
        //    string query = baseSelectFromSql + @" WHERE cs.subscription_data_set_id = :subscriptionDataSetId AND cs.run_id = :runId  ";
        //    var recordResult = unitOfWork.Connection.Query<CacheEntry<TEntity>>(query, new { subscriptionDataSetId, runId })
        //        .Select(r => new DeltaSnapshotCacheRowType<long, TEntity>(r.CacheSnapshotId, r.SubscriptionDataSetId, r.RunId, r.EntityIdentifier, r.EntityDeltaCode, r.EntityDeltaDate, r.EntityDataCurrent, r.EntityDataPrevious));
        //    return recordResult;
        //}

        public FindCacheEntryResultType<long, TEntity> GetNewestById<TCachePrimaryKey, TEntity>(long subscriptionDataSetId, string entityIdentifier)
                where TEntity : class, IDataSetEntity, new() {
            string baseQuery = baseSelectFromSql + @" WHERE cs.subscription_data_set_id = :subscriptionDataSetId AND cs.entity_identifier = :entityIdentifier ORDER BY cs.entity_delta_date DESC ";
            var query = $"SELECT * FROM ({baseQuery}) WHERE ROWNUM = 1 ";
            var queryResult = unitOfWork.Connection.Query<CacheEntry<TEntity>>(query, new { subscriptionDataSetId, entityIdentifier })
                .Select(r => new DeltaSnapshotCacheRowType<long, TEntity>(r.CacheSnapshotId, r.SubscriptionDataSetId, r.RunId, r.EntityIdentifier, r.EntityDeltaCode, r.EntityDeltaDate, r.EntityDataCurrent, r.EntityDataPrevious))
                .FirstOrDefault();
            return queryResult == null ? Api.Common.CreateFindCacheEntryResultFailure<long, TEntity>() : Api.Common.CreateFindCacheEntryResultSuccess<long, TEntity>(queryResult);
        }

        public bool GetOldestByIdWithLock<TEntity>(long subscriptionDataSetId, string entityIdentifier) where TEntity : class, IDataSetEntity, new() {
            string query = baseSelectFromSql
                + @" WHERE cs.subscription_data_set_id = :subscriptionDataSetId AND cs.entity_identifier = :entityIdentifier "
                    + @" AND cs.run_id = (SELECT MIN(run_id) FROM dlta_cache_snapshot cs WHERE cs.subscription_data_set_id = :subscriptionDataSetId  AND cs.entity_identifier = :entityIdentifier) "
                + @" FOR UPDATE ";  // lock
            var queryResult = unitOfWork.Connection.Query<CacheEntry<TEntity>>(query, new { subscriptionDataSetId, entityIdentifier })
                .Select(r => new DeltaSnapshotCacheRowType<long, TEntity>(r.CacheSnapshotId, r.SubscriptionDataSetId, r.RunId, r.EntityIdentifier, r.EntityDeltaCode, r.EntityDeltaDate, r.EntityDataCurrent, r.EntityDataPrevious))
                .FirstOrDefault();
            return queryResult != null;
        }

        public FindCacheNewestRunIdResultType GetRunIdMaxOfDataSet(long subscriptionDataSetId) {
            Console.WriteLine($"CONSUMER: called GetRunIdMaxOfDataSet()");
            var query = @"  SELECT  MAX(run_id)
                            FROM    dlta_cache_snapshot
                            WHERE   subscription_data_set_id = :subscriptionDataSetId ";
            var runId = unitOfWork.Connection.Query<long?>(query, new { subscriptionDataSetId }).FirstOrDefault();
            return runId.HasValue ? Api.Subscriber.CreateFindCacheNewestRunIdResultSuccess(runId.Value) : Api.Subscriber.CreateFindCacheNewestRunIdResultFailure();
        }

        public long Insert<TCachePrimaryKey, TEntity>(DeltaSnapshotCacheRowType<long, TEntity> cacheEntry) 
                where TEntity : class, IDataSetEntity, new() {
            try {
                var cacheSnapshotId = DatabaseUtil.GetNextVal("DLTA_CACHE_SNAPSHOT_S", unitOfWork.Connection);
                var sql = @"INSERT INTO dlta_cache_snapshot (
                            cache_snapshot_id,
                            subscription_data_set_id,
                            run_id,
                            entity_identifier,
                            entity_delta_code,
                            entity_delta_date,
                            entity_data_current,
                            entity_data_previous )
                        VALUES (
                            :cacheSnapshotId,
                            :SubscriptionDataSetId,
                            :RunId,
                            :EntityIdentifier,
                            :EntityDeltaCode,
                            :EntityDeltaDate,
                            :EntityDataCurrent,
                            :EntityDataPrevious ) ";
                unitOfWork.Connection.Execute(sql, new {
                    cacheSnapshotId,
                    cacheEntry.SubscriptionDataSetId,
                    cacheEntry.RunId,
                    cacheEntry.EntityIdentifier,
                    cacheEntry.EntityDeltaCode,
                    cacheEntry.EntityDeltaDate,
                    cacheEntry.EntityDataCurrent,
                    cacheEntry.EntityDataPrevious
                });
                return cacheSnapshotId;
            } catch (Exception ex) {
                Console.WriteLine($"CONSUMER: {nameof(Insert)} {cacheEntry.EntityIdentifier} {ex.Message}");
                Console.WriteLine($"CONSUMER: {nameof(Insert)} {cacheEntry.EntityIdentifier} {ex.StackTrace}");
                throw;
            }
        }

        public void Update<TCachePrimaryKey, TEntity>(DeltaSnapshotCacheRowType<long, TEntity> cacheEntry) 
                where TEntity : class, IDataSetEntity, new() {
            try {
                string sql = @" UPDATE  dlta_cache_snapshot 
                            SET     subscription_data_set_id    = :SubscriptionDataSetId,
                                    run_id                      = :RunId,
                                    entity_identifier           = :EntityIdentifier,
                                    entity_delta_code           = :EntityDeltaCode,
                                    entity_delta_date           = :EntityDeltaDate,
                                    entity_data_current         = :EntityDataCurrent,
                                    entity_data_previous        = :EntityDataPrevious
                            WHERE   cache_snapshot_id = :PrimaryKey ";
                unitOfWork.Connection.Execute(sql, new {
                    cacheEntry.SubscriptionDataSetId,
                    cacheEntry.RunId,
                    cacheEntry.EntityIdentifier,
                    cacheEntry.EntityDeltaCode,
                    cacheEntry.EntityDeltaDate,
                    cacheEntry.EntityDataCurrent,
                    cacheEntry.EntityDataPrevious,
                    cacheEntry.PrimaryKey
                });
            } catch (Exception ex) {
                Console.WriteLine($"CONSUMER: {nameof(Update)} {cacheEntry.EntityIdentifier} {ex.Message}");
                Console.WriteLine($"CONSUMER: {nameof(Update)} {cacheEntry.EntityIdentifier} {ex.StackTrace}");
                throw;
            }
        }

        public void DeleteSubscriptionDataSet(long subscriptionDataSetId) {
            string sql = @" DELETE FROM dlta_cache_snapshot 
                            WHERE       subscription_data_set_id = :subscriptionDataSetId ";
            unitOfWork.Connection.Execute(sql, new { subscriptionDataSetId });
        }
    }
}