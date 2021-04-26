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
using DeltaSnapshot;

namespace TesterCache {
    public class Run {
        public Run(int subscriptionDataSetId, RunModeType runMode) {
                SubscriptionDataSetId = subscriptionDataSetId;
                RunMode = runMode.ToString();
                StatusCode = @"RUNNING";
                StartDate = DateTimeOffset.Now;
        }

        public long? RunId { get; set; }
        public int SubscriptionDataSetId { get; set; }
        public string RunMode { get; set; }
        public string StatusCode { get; set; }
        public string StatusMessage { get; set; }
        public int DataSetCount { get; set; }
        public int DeltaCount { get; set; }
        public DateTimeOffset StartDate { get; set; }
        public DateTimeOffset? EndDate { get; set; }
    }
}