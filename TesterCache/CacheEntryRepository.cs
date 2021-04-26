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
        private readonly string baseSelectFromSql = @"SELECT    cs.subscription_data_set_id AS SubscriptionDataSetId,
                                                                cs.run_id AS RunId,
                                                                cs.entity_identifier AS EntityIdentifier,
                                                                cs.entity_delta_code AS EntityDeltaCode,
                                                                cs.entity_delta_date AS EntityDeltaDate,
                                                                cs.entity_data_current AS EntityDataCurrent,
                                                                cs.entity_data_previous AS EntityDataPrevious "
                                                    + baseFromSql;
        public void BeginTransaction() { unitOfWork.Begin(); }
        public void CommitTransaction() { unitOfWork.Commit(); }
        public void RollbackTransaction() { unitOfWork.Rollback(); }

        public IEnumerable<ICacheEntryType<T>> GetByRunIdExcludingDeltaState(int subscriptionDataSetId, long runId, string deltaStateCodeExclude) {
            Console.WriteLine($"called CacheEntryRepository.GetByRunIdExcludingDeltaState {deltaStateCodeExclude}");
            string query = baseSelectFromSql + @" WHERE cs.subscription_data_set_id = :subscriptionDataSetId AND cs.run_id = :runId AND cs.entity_delta_code != :deltaStateCodeExclude ";
            return unitOfWork.Connection.Query<CacheEntry<T>>(query, new { subscriptionDataSetId, runId, deltaStateCodeExclude });
        }

        public FindCacheEntryResultType<T> GetLatestById(Int32 subscriptionDataSetId, string entityIdentifier) {
            string baseQuery = baseSelectFromSql + @" WHERE cs.subscription_data_set_id = :subscriptionDataSetId AND cs.entity_identifier = :entityIdentifier ORDER BY cs.entity_delta_date DESC ";
            var query = $"SELECT * FROM ({baseQuery}) WHERE ROWNUM = 1 ";
            var queryResult = unitOfWork.Connection.Query<CacheEntry<T>>(query, new { subscriptionDataSetId, entityIdentifier }).FirstOrDefault();
            return queryResult == null ? Api.Subscriber.CreateFindCacheEntryResultFailure<T>() : Api.Subscriber.CreateFindCacheEntryResultSuccess<T>(queryResult);
        }

        public FindCacheLatestRunIdResultType GetRunIdMax(Int32 subscriptionDataSetId) {
            var query = @"  SELECT  MAX(run_id)
                            FROM    dlta_cache_snapshot
                            WHERE   subscription_data_set_id = :subscriptionDataSetId ";
            var runId = unitOfWork.Connection.Query<long?>(query, new { subscriptionDataSetId }).FirstOrDefault();
            return runId.HasValue ? Api.Subscriber.CreateFindCacheLatestRunIdResultSuccess(runId.Value) : Api.Subscriber.CreateFindCacheLatestRunIdResultFailure();
        }

        public void Insert(ICacheEntryType<T> cacheEntry) {
            //cacheEntry.CacheEntryId = DatabaseUtil.GetNextVal("DLTA_CACHE_ENTRY_S", unitOfWork.Connection);
            var sql = @"INSERT INTO dlta_cache_snapshot (
                            subscription_data_set_id,
                            run_id,
                            entity_identifier,
                            entity_delta_code,
                            entity_delta_date,
                            entity_data_current,
                            entity_data_previous )
                        VALUES (
                            :SubscriptionDataSetId,
                            :RunId,
                            :EntityIdentifier,
                            :EntityDeltaCode,
                            :EntityDeltaDate,
                            :EntityDataCurrent,
                            :EntityDataPrevious ) ";
            unitOfWork.Connection.Execute(sql, new {
                //cacheEntry.CacheEntryId,
                cacheEntry.SubscriptionDataSetId,
                cacheEntry.RunId,
                cacheEntry.EntityIdentifier,
                cacheEntry.EntityDeltaCode,
                cacheEntry.EntityDeltaDate,
                cacheEntry.EntityDataCurrent,
                cacheEntry.EntityDataPrevious
            });
        }

        public void DeleteDeltaStateLessThanRunId(int subscriptionDataSetId, string deltaStateCode, long runId) {
            string sql = @" DELETE FROM dlta_cache_snapshot 
                            WHERE       subscription_data_set_id = :subscriptionDataSetId 
                                AND     entity_delta_code = :deltaStateCode
                                AND     run_id < :runId ";
            unitOfWork.Connection.Execute(sql, new { subscriptionDataSetId, deltaStateCode, runId });
        }
    }
}