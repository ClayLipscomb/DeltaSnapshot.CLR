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
using System.Data;
using Dapper;
using Oracle.ManagedDataAccess.Client;

namespace TesterCs.Database {
    /// <summary>
    /// Base repository used for NIS database
    /// </summary>
    internal static class DatabaseUtil {
        internal static IDbConnection GetConnection() {
            var connection = new OracleConnection("data source=XE;user id=DLTA;password=dlta;enlist=false");
            connection.Open();
            return connection;
        }

        internal static long GetNextVal(string sequenceName, IDbConnection conn) {
            return (long)conn.Query<long>($"SELECT {sequenceName}.NEXTVAL FROM DUAL").SingleOrDefault();
        }
    }
}