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

namespace TesterCs.Database {
    public class UnitOfWork : IUnitOfWork {
        internal UnitOfWork(IDbConnection connection) {
            _id = Guid.NewGuid();
            _connection = connection;
        }

        readonly IDbConnection _connection = null;
        IDbTransaction _transaction = null;
        Guid _id = Guid.Empty;

        IDbConnection IUnitOfWork.Connection => _connection; 
        IDbTransaction IUnitOfWork.Transaction => _transaction; 
        Guid IUnitOfWork.Id => _id; 
        public void Begin() {
            _transaction = _connection.BeginTransaction();
        }

        public void Commit() {
            _transaction.Commit();
            Dispose();
        }

        public void Rollback() {
            _transaction.Rollback();
            Dispose();
        }

        public void Dispose() {
            if (_transaction != null) _transaction.Dispose();
            _transaction = null;
        }

        public bool IsConnectionStale() =>
            _connection == null || _connection?.State == ConnectionState.Broken || _connection?.State == ConnectionState.Closed;
    }
}