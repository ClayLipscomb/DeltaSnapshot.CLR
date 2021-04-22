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

namespace TesterCs.Database {
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
        private const string baseFromSql = @" FROM dlta_cache_entry ce ";
        private readonly string baseSelectFromSql = @"SELECT    ce.cache_entry_id AS CacheEntryId, 
                                                                ce.data_set_id AS DataSetId,
                                                                ce.run_id AS RunId,
                                                                ce.entity_identifier AS EntityIdentifier,
                                                                ce.entity_delta_code AS EntityDeltaCode,
                                                                ce.entity_delta_date AS EntityDeltaDate,
                                                                ce.entity_data_current AS EntityDataCurrent,
                                                                ce.entity_data_previous AS EntityDataPrevious "
                                                    + baseFromSql;
        public void BeginTransaction() { unitOfWork.Begin(); }
        public void CommitTransaction() { unitOfWork.Commit(); }
        public void RollbackTransaction() { unitOfWork.Rollback(); }

        public IEnumerable<ICacheEntryType<T>> GetByRunIdExcludingDeltaState(int dataSetId, long runId, string deltaStateCodeExclude) {
            Console.WriteLine($"called CacheEntryRepository.GetByRunIdExcludingDeltaState {deltaStateCodeExclude}");
            string query = baseSelectFromSql + @" WHERE ce.data_set_id = :dataSetId AND ce.run_id = :runId AND ce.entity_delta_code != :deltaStateCodeExclude ";
            return unitOfWork.Connection.Query<CacheEntry<T>>(query, new { dataSetId, runId, deltaStateCodeExclude });
        }

        public FindCacheEntryResultType<T> GetLatestById(Int32 dataSetId, string entityIdentifier) {
            string baseQuery = baseSelectFromSql + @" WHERE ce.data_set_id = :dataSetId AND ce.entity_identifier = :entityIdentifier ORDER BY ce.entity_delta_date DESC ";
            var query = $"SELECT * FROM ({baseQuery}) WHERE ROWNUM = 1 ";
            var queryResult = unitOfWork.Connection.Query<CacheEntry<T>>(query, new { dataSetId, entityIdentifier }).FirstOrDefault();
            return queryResult == null ? Api.Consumer.CreateFindCacheEntryResultFailure<T>() : Api.Consumer.CreateFindCacheEntryResultSuccess<T>(queryResult);
        }

        public FindCacheLatestRunIdResultType GetRunIdMax(Int32 dataSetId) {
            var query = @"  SELECT  MAX(run_id)
                            FROM    dlta_cache_entry
                            WHERE   data_set_id = :dataSetId ";
            var runId = unitOfWork.Connection.Query<long?>(query, new { dataSetId }).FirstOrDefault();
            return runId.HasValue ? Api.Consumer.CreateFindCacheLatestRunIdResultSuccess(runId.Value) : Api.Consumer.CreateFindCacheLatestRunIdResultFailure();
        }

        public void Insert(ICacheEntryType<T> cacheEntry) {
            cacheEntry.CacheEntryId = DatabaseUtil.GetNextVal("DLTA_CACHE_ENTRY_S", unitOfWork.Connection);
            var sql = @"INSERT INTO dlta_cache_entry (
                            cache_entry_id,
                            data_set_id,
                            run_id,
                            entity_identifier,
                            entity_delta_code,
                            entity_delta_date,
                            entity_data_current,
                            entity_data_previous )
                        VALUES (
                            :CacheEntryId,
                            :DataSetId,
                            :RunId,
                            :EntityIdentifier,
                            :EntityDeltaCode,
                            :EntityDeltaDate,
                            :EntityDataCurrent,
                            :EntityDataPrevious ) ";
            unitOfWork.Connection.Execute(sql, new {
                cacheEntry.CacheEntryId,
                cacheEntry.DataSetId,
                cacheEntry.RunId,
                cacheEntry.EntityIdentifier,
                cacheEntry.EntityDeltaCode,
                cacheEntry.EntityDeltaDate,
                cacheEntry.EntityDataCurrent,
                cacheEntry.EntityDataPrevious
            });
        }

        //public void Update(ICacheEntryType<T> cacheEntry) {
        //    string sql = @" UPDATE  dlta_cache_entry 
        //                    SET     data_set_id             = :DataSetId,
        //                            run_id                  = :RunId,
        //                            entity_identifier       = :EntityIdentifier,
        //                            entity_delta_code       = :EntityDeltaCode,
        //                            entity_delta_date       = :EntityDeltaDate,
        //                            entity_data_current     = :EntityDataCurrent,
        //                            entity_data_previous    = :EntityDataPrevious
        //                    WHERE cache_entry_id = :CacheEntryId ";
        //    unitOfWork.Connection.Execute(sql, new {
        //        cacheEntry.DataSetId,
        //        cacheEntry.RunId,
        //        cacheEntry.EntityIdentifier,
        //        cacheEntry.EntityDeltaCode,
        //        cacheEntry.EntityDeltaDate,
        //        cacheEntry.EntityDataCurrent,
        //        cacheEntry.EntityDataPrevious,
        //        cacheEntry.CacheEntryId
        //    });
        //}

        public void DeleteDeltaStateLessThanRunId(int dataSetId, string deltaStateCode, long runId) {
            string sql = @" DELETE FROM dlta_cache_entry 
                            WHERE       data_set_id = :dataSetId 
                                AND     entity_delta_code = :deltaStateCode
                                AND     run_id < :runId ";
            unitOfWork.Connection.Execute(sql, new { dataSetId, deltaStateCode, runId });
        }
    }
}