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
using System.Linq;
using System.Collections.Generic;
using Dapper;
using System.Data;

namespace TesterCache {
    public class RunRepository : IDisposable {
        protected IUnitOfWork unitOfWork;
        public void Dispose() { unitOfWork.Dispose(); }

        public RunRepository(IUnitOfWork unitOfWork) {
            this.unitOfWork = unitOfWork;
        }

        public long GetNewRunId() { 
            return DatabaseUtil.GetNextVal(@"DLTA_RUN_S", unitOfWork.Connection);
        }

        public long Insert(Run run) {
            run.RunId = GetNewRunId();// DatabaseUtil.GetNextVal(@"DLTA_RUN_S", conn);
            var sql = @"INSERT INTO dlta_run (
                            run_id,
                            subscription_data_set_id,
                            run_mode,
                            status_code,
                            status_message,
                            data_set_count,
                            delta_count,
                            start_date,
                            end_date)
                        VALUES (
                            :RunId,
                            :SubscriptionDataSetId,
                            :RunMode,
                            :StatusCode,
                            :StatusMessage,
                            :DataSetCount,
                            :DeltaCount,
                            :StartDate,
                            :EndDate )";
            unitOfWork.Connection.Execute(sql, new {
                run.RunId,
                run.SubscriptionDataSetId,
                run.RunMode,
                run.StatusCode,
                run.StatusMessage, 
                run.DataSetCount,
                run.DeltaCount,
                run.StartDate,
                run.EndDate
            });
            return run.RunId.Value;
        }

        public void Update(long runId, string statusCode, string statusMessage, int dataSetCount, int deltaCount) {
            string sql = @"UPDATE dlta_run SET
                                status_code = :StatusCode,
                                status_message = :StatusMessage,
                                data_set_count = :DataSetCount,
                                delta_count = :DeltaCount,
                                end_date = :EndDate
                            WHERE run_id = :RunId ";
            unitOfWork.Connection.Execute(sql, new {
                statusCode,
                statusMessage,
                dataSetCount,
                deltaCount,
                EndDate = DateTimeOffset.Now,
                runId
            });
        }
    }
}
