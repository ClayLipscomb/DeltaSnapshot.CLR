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
using DeltaSnapshot;

namespace TesterCache {
    public static class RunService {
        public static long StartRun(Int32 subscriptionId, RunModeType runMode) {
            using RunRepository repo = new RunRepository(new UnitOfWork(DatabaseUtil.GetConnection()));
            return repo.Insert(new Run(subscriptionId, runMode));
        }

        public static void CompleteRun(Int64 runId, bool isSuccess, string statusMessage, int dataSetCount, int deltaCount) {
            using RunRepository repo = new RunRepository(new UnitOfWork(DatabaseUtil.GetConnection()));
            repo.Update(runId, isSuccess ? "SUCCESS" : "FAILURE", statusMessage, dataSetCount, deltaCount);
        }
    }
}