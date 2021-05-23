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

namespace DeltaSnapshot

open System

#if DEBUG
module public ApiTest =
    let private isCacheActionUpdate result = (result.CacheActionPending = Update)

    // helper funcs
    let deltaSnapshotCacheRowCreateAdd runIdValue subscriptionDataSetIdValue entityLatest =
        DeltaSnapshotCacheRow.createAdd (RunId.create runIdValue) (SubscriptionDataSetId.create subscriptionDataSetIdValue) entityLatest    
    let deltaSnapshotCacheRowToDel cacheRow runIdValue = 
        DeltaSnapshotCacheRow.toDel cacheRow (RunId.create runIdValue)
    let deltaSnapshotCacheRowToCur cacheRow runIdValue = 
        DeltaSnapshotCacheRow.toCur cacheRow (RunId.create runIdValue)
    let deltaSnapshotCacheRowToUpd cacheRow entityLatest runIdValue = 
        DeltaSnapshotCacheRow.toUpd cacheRow entityLatest (RunId.create runIdValue)

    // public proxies for unit tests
    let deltaStateFromStr = DeltaState.fromStr

    let testProcessDataSetEntity<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (runIdValue, subscriptionDataSetIdValue) (isEqualByValue: IsEqualByValueDelegate<'TEntity>) (entity: 'TEntity, cacheEntryRowOption: DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> option) = 
        let isEqual = fun (entity1, entity2) -> isEqualByValue.Invoke (entity1, entity2)
        let result = DeltaSnapshotCore.processDataSetEntity (SubscriptionDataSetId.create subscriptionDataSetIdValue, RunId.create runIdValue) (isEqual) false (entity, cacheEntryRowOption)
        (result.CacheRowProcessed |> Processed.value, isCacheActionUpdate result)    

    let testProcessNonDeleteCacheEntryAsDelete<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (runIdValue: RunIdPrimitive) (cacheEntryNonDelete: DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>) =
        let result = processNonDeleteCacheEntryAsDelete (RunId.create runIdValue) false cacheEntryNonDelete 
        match result with 
            | Some r -> Some (r.CacheRowProcessed |> Processed.value, isCacheActionUpdate r)  
            | None -> None

    let testMessageOfCacheRowPersisted isAll cacheRow = 
        DeltaSnapshotMessage.ofCacheRowPersisted (if isAll then All else DeltasOnly) (PersistedType cacheRow) 

    let testMessageOfCacheRowPersistedScopeDeltasOnly cacheRow = 
        DeltaSnapshotMessage.ofCacheRowPersisted DeltasOnly (PersistedType cacheRow) 
    let testMessageOfCacheRowPersistedScopeAll cacheRow = 
        DeltaSnapshotMessage.ofCacheRowPersisted All (PersistedType cacheRow) 

#endif