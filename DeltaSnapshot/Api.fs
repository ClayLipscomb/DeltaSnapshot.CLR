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

[<AutoOpen>]
module internal ApiUtil =
    //let nullArg name message = raise new System.ArgumentNullException(name, message)
    let nullableToOption = function | null -> None | x -> Some x
    let optionToNullable = function | Some x -> x | None -> null
    let ifNoneThrowElseValue (optionType, errorMsg) = if optionType |> Option.isNone then failwith errorMsg else optionType.Value 
    let failwithNullOrEmpty desc = failwith $"{desc} cannot be null or empty" 

module public Api =
    module Common = 
        let CreateFindCacheEntryResultSuccess<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
                (cacheEntry: DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>) = 
            cacheEntry |> FindCacheEntryResultType.FoundCacheEntry
        let CreateFindCacheEntryResultFailure () = 
            FindCacheEntryResultType.NotFoundCacheEntry

    module Subscriber =
        let CreateCacheOperationBatch<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
                ( beginTransaction: BeginTransactionDelegate
                , commitTransaction: CommitTransactionDelegate
                , rollbackTransaction: RollbackTransactionDelegate
                , findRunIdNewestOfDataSet: FindNewestRunIdOfDataSetCacheDelegate
                , insert: InsertDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>
                , update: UpdateDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>
                , findEntityNewest: FindNewestDataSetEntityByIdCacheDelegate<'TCachePrimaryKey, 'TEntity>
                , getDataSetRunExcludeDeltaState: GetDataSetRunExcludeDeltaStateCacheDelegate<'TCachePrimaryKey, 'TEntity> ) = 
            CacheOperationBatch.create (beginTransaction, commitTransaction, rollbackTransaction, findRunIdNewestOfDataSet, insert, update, findEntityNewest, getDataSetRunExcludeDeltaState)    

        let CreateFindCacheNewestRunIdResultSuccess (runId: RunIdPrimitive) = 
            runId |> FindCacheNewestRunIdResultType.FoundRunId
        let CreateFindCacheNewestRunIdResultFailure () = 
            FindCacheNewestRunIdResultType.NotFoundRunId

        let PullBatchDeltas<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
                ( subscription: ISubscription
                , runIdNew: RunIdPrimitive
                , pullPublisherDataSet: PullPublisherDataSetDelegate<'TEntity>
                , emptyDataSetGetDeltasStrategy: EmptyPublisherDataSetGetDeltasStrategyType
                , isEqualByValue: IsEqualByValueDelegate<'TEntity>
                , cacheEntryOperation: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity> ) = 
            pullBatchDeltas (subscription) (RunId.create runIdNew) pullPublisherDataSet emptyDataSetGetDeltasStrategy isEqualByValue cacheEntryOperation

        let PullBatchDeltasAndCurrents<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
                ( subscription: ISubscription
                , runIdNew: RunIdPrimitive
                , pullPublisherDataSet: PullPublisherDataSetDelegate<'TEntity>
                , emptyDataSetGetDeltasStrategy: EmptyPublisherDataSetGetDeltasStrategyType
                , isEqualByValue: IsEqualByValueDelegate<'TEntity>
                , cacheEntryOperation: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity> ) = 
            pullBatchDeltasAndCurrents (subscription) (RunId.create runIdNew) pullPublisherDataSet emptyDataSetGetDeltasStrategy isEqualByValue cacheEntryOperation

    module Publisher =
        let CreateCacheOperationEvent<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
                ( beginTransaction: BeginTransactionDelegate
                , commitTransaction: CommitTransactionDelegate
                , rollbackTransaction: RollbackTransactionDelegate
                , insert: InsertDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>
                , lockOldest: LockOldestDataSetEntityByIdCacheDelegate<'TEntity>
                , findEntityNewest: FindNewestDataSetEntityByIdCacheDelegate<'TCachePrimaryKey, 'TEntity> ) = 
            CacheOperationEvent.create (beginTransaction, commitTransaction, rollbackTransaction, insert, lockOldest, findEntityNewest)

        let PushEvent (eventCacheLockingStrategyType: EventCacheLockingStrategyType) = 1
