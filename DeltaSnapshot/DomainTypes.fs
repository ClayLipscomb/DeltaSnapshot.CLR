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

type internal SnapshotDatePrimitive = DateTimeOffset

type internal CountPrimitive = Int32
type internal DataSetCountType = internal DataSetCountType of CountPrimitive
type internal DeltaCountType = internal DeltaCountType of CountPrimitive

type internal RunIdPrimitive = Int64
type internal RunIdType = internal RunIdType of RunIdPrimitive

type internal SubscriptionDataSetIdPrimitive = Int32
type internal SubscriptionDataSetIdType = internal SubscriptionDataSetIdType of SubscriptionDataSetIdPrimitive

type internal EntityIdentifierPrimitive = string
type internal EntityIdentifierType = EntityIdentifierPrimitive

type internal DeltaStatePrimitive = string

[<Struct;NoEquality;NoComparison>]
type internal DataSetRunType = { SubscriptionDataSetId: SubscriptionDataSetIdType; RunIdCurr: RunIdType; RunIdPrev: RunIdType option }

type internal DeltaSnapshotCacheRowActionType = | Insert | Update

///////////////
// Public types 

[<Struct;NoComparison>]
type public DeltaStateType = | CUR | ADD | UPD | DEL with
    override this.ToString() = this |> Union.fromDuCaseToString
[<Struct;NoEquality;NoComparison>]
type public RunModeType = | SET_DELTA | SET_ALL (*| SET_RESET | ATC_ALL | ATC_DELTA | ATC_RESET*) with
    override this.ToString() = this |> Union.fromDuCaseToString

/// A .NET data set entity that can be tracked for deltas
type public IDataSetEntity = 
    abstract member Identifier: EntityIdentifierPrimitive with get 

/// interface of a subscription
type public ISubscription =
    abstract member SubscriptionDataSetId: SubscriptionDataSetIdPrimitive with get
    abstract member SubscriptionDataSetFilter: string with get

/// record of delta snapshot (message) 
[<NoEquality;NoComparison>]
type public DeltaSnapshotMessage<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    { Id: EntityIdentifierType; Delta: DeltaStateType; Date: SnapshotDatePrimitive; IsFull: bool; Cur: 'TEntity; Prv: 'TEntity }

/// class of delta snapshot (message) 
//[<NoEquality;NoComparison>]
//type public DeltaSnapshotType<'TEntity when 'TEntity :> IEntityTrackable and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
//        internal (id: EntityIdentifierType, delta: DeltaStateType, isFull: bool, curr: 'TEntity, prev: 'TEntity) = 
//    member this.Id with get() = id          // instead of "Identifier" in order to conserve serialization size
//    member this.Delta with get() = delta 
//    member this.SnapshotDate with get() :SnapshotDatePrimitive = DateTimeOffset.Now
//    member this.IsFull with get() = isFull
//    member this.Cur with get() = curr
//    member this.Prv with get() = prev

/// interface of a cache row 
type public ICacheEntryType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    /// Set by subscriber upon database insert
    //abstract member CacheEntryId: CacheEntryIdPrimitive with get, set
    abstract member SubscriptionDataSetId: SubscriptionDataSetIdPrimitive  with get, set
    abstract member RunId: RunIdPrimitive with get, set
    abstract member EntityIdentifier: EntityIdentifierType with get, set
    abstract member EntityDeltaCode: DeltaStatePrimitive with get, set
    abstract member EntityDeltaDate: SnapshotDatePrimitive with get, set
    abstract member EntityDataCurrent: 'TEntity with get, set
    abstract member EntityDataPrevious: 'TEntity with get, set

/// class of a cache row
[<NoEquality;NoComparison>]
type public CacheEntryType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
        internal (subscriptionDataSetId, runId, entityIdentifier, deltaState: DeltaStateType, deltaDate, rowCurrent, rowPrevious) =
    interface ICacheEntryType<'TEntity> with
        //member val CacheEntryId = 0L with get, set
        member val SubscriptionDataSetId = subscriptionDataSetId with get, set
        member val RunId = runId with get, set
        member val EntityIdentifier = entityIdentifier with get, set
        member val EntityDeltaCode = deltaState.ToString() with get, set
        member val EntityDeltaDate = deltaDate with get, set 
        member val EntityDataCurrent = rowCurrent with get, set
        member val EntityDataPrevious = rowPrevious with get, set

/// record of cqche row 
[<NoEquality;NoComparison>]
type public DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    {   PrimaryKey: 'TCachePrimaryKey; SubscriptionDataSetId: SubscriptionDataSetIdPrimitive; RunId: RunIdPrimitive; EntityIdentifier: EntityIdentifierPrimitive; 
        EntityDeltaCode: DeltaStatePrimitive; EntityDeltaDate: SnapshotDatePrimitive; EntityDataCurrent: 'TEntity; EntityDataPrevious: 'TEntity }

[<Struct;NoEquality;NoComparison>]
type public FindCacheEntryResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    internal | NotFoundCacheEntry | FoundCacheEntry of ICacheEntryType<'TEntity> 
[<Struct;NoEquality;NoComparison>]
type public FindCacheLatestRunIdResultType = 
    internal | NotFoundRunId | FoundRunId of RunIdType
[<Struct;NoEquality;NoComparison>]
type public EmptyDataSetGetDeltasStrategyType = 
    | DefaultProcessing | RunSuccessWithBypass | RunFailure 

/// Record of run result
[<NoEquality;NoComparison>]
type public DeltaRunResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    { IsSuccess: bool; RunId: RunIdPrimitive; ErrorMsgs: string seq; DeltaSnapshots: DeltaSnapshotMessage<'TEntity> seq; DataSetCount: CountPrimitive; DeltaCount: CountPrimitive }

// Delegates 
/// Determines whether two entities are equal by structure/value
type public IsEqualDelegate<'TEntity when 'TEntity :> IDataSetEntity> = 
    delegate of 'TEntity * 'TEntity -> bool
/// Retrieve entire publisher data set
type public PullPublisherDataSetDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of ISubscription -> 'TEntity seq
/// Begin a cache transaction
type public BeginTransactionDelegate = 
    delegate of unit -> unit
/// Commit existing cache transaction
type public CommitTransactionDelegate = 
    delegate of unit -> unit
/// Rollback existing cache transaction
type public RollbackTransactionDelegate = 
    delegate of unit -> unit
/// Insert row into cache
type public InsertCacheEntryDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of ICacheEntryType<'TEntity> -> unit
/// Delete all cache entries in a subscription data set with a specific delta state and prior to a specific run id
type public DeleteCacheEntryDeltaStatePriorToRunIdDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of SubscriptionDataSetIdPrimitive * DeltaStatePrimitive * RunIdPrimitive -> unit
/// Find most recent cache entry row in subscription data set with entity identifier
type public FindLatestCacheEntryDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of SubscriptionDataSetIdPrimitive * EntityIdentifierType -> FindCacheEntryResultType<'TEntity>
/// Retrieve all cache entry rows in subscription data set by run id excluding a specifc delta state
type public GetCacheEntryByRunIdExcludeDeltaStateDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    delegate of SubscriptionDataSetIdPrimitive * RunIdPrimitive * DeltaStatePrimitive -> ICacheEntryType<'TEntity> seq
/// Find most recent cache row run id for a subsription data set id
type public FindCacheEntryLatestRunIdDelegate =
    delegate of SubscriptionDataSetIdPrimitive -> FindCacheLatestRunIdResultType

/// Record of all necessary cache operations
[<Struct;NoEquality;NoComparison>]
type public CacheEntryOperation<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    {   BeginTransaction: BeginTransactionDelegate;
        CommitTransaction: CommitTransactionDelegate;
        RollbackTransaction: RollbackTransactionDelegate;
        Insert: InsertCacheEntryDelegate<'TEntity>; 
        DeleteDeltaStatePriorToRunId: DeleteCacheEntryDeltaStatePriorToRunIdDelegate<'TEntity>; 
        FindLatest: FindLatestCacheEntryDelegate<'TEntity>; 
        GetRunIdLatest: FindCacheEntryLatestRunIdDelegate;
        GetByRunIdExcludeDeltaState: GetCacheEntryByRunIdExcludeDeltaStateDelegate<'TEntity>; }