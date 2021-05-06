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

namespace TesterCs {
    public class TesterEntity : IDataSetEntity {
        public string Identifier { get; set; }
        public long? LongValue { get; set; }
        public string StringValue { get; set; }
        public DateTimeOffset? DateTimeOffsetValue { get; set; }
        public bool? BoolValue { get; set; }
        public static bool IsEqual(TesterEntity dt1, TesterEntity dt2) {
            if (dt1 == null || dt2 == null) return false;
            if (ReferenceEquals(dt1, dt2)) return true;

            return (dt1.Identifier == dt2.Identifier
                    && dt1.LongValue == dt2.LongValue
                    && dt1.StringValue == dt2.StringValue
                    && dt1.DateTimeOffsetValue == dt2.DateTimeOffsetValue
                    && dt1.BoolValue == dt2.BoolValue);
        }
    }
}