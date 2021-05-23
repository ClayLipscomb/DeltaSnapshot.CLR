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

type internal ProcessedType<'T> = ProcessedType of 'T
type internal PersistedType<'T> = PersistedType of 'T

type internal DataSetCountType = internal DataSetCountType of CountPrimitive
type internal DeltaCountType = internal DeltaCountType of CountPrimitive
type internal RunIdType = internal RunIdType of RunIdPrimitive
type internal SubscriptionDataSetIdType = internal SubscriptionDataSetIdType of SubscriptionDataSetIdPrimitive
type internal EntityIdentifierType = EntityIdentifierPrimitive

type internal DeltaSnapshotCacheRowActionType = | Insert | Update
type internal RunResultScopeType = | DeltasOnly | All
type internal DeltaSnapshotPatternType = | Batch | Event
type internal TransactionStartedStateType = TransactionStartedState

[<Struct;NoEquality;NoComparison>]
type internal DataSetRunType = 
    { SubscriptionDataSetId: SubscriptionDataSetIdType; RunIdCurr: RunIdType; RunIdPrev: RunIdType option }

type internal PersistProcessedCacheRowFuncType<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    ProcessedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>> -> PersistedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>>

type internal InsertUpdateCacheType<'TCachePrimaryKey, 'TEntity  when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    { Insert: PersistProcessedCacheRowFuncType<'TCachePrimaryKey, 'TEntity>; Update: PersistProcessedCacheRowFuncType<'TCachePrimaryKey, 'TEntity> }

type internal DataSetProcessResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    { DataSetCount: DataSetCountType; DataSetEntityIds: EntityIdentifierType[]; DeltaSnapshotMessages: DeltaSnapshotMessage<'TEntity> seq }

type internal CacheRowPendingPersistenceType<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    { CacheRowProcessed: ProcessedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>>; CacheActionPending: DeltaSnapshotCacheRowActionType; ProcessedMessage: string option }

type internal ProccessedCacheRowLogInfoType = 
    { IsRowFound: bool; IsEqualsOption: bool option; PriorDeltaStateOption: DeltaStateType option }