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

/////////////////////////////////////////////
// Internal type aliases affecting public API        
type internal RunIdPrimitive = Int64
type internal SubscriptionDataSetIdPrimitive = Int32
type internal EntityIdentifierPrimitive = string
type internal SnapshotDatePrimitive = DateTimeOffset
type internal CountPrimitive = Int32
type internal DeltaStatePrimitive = string

/////////////////
// Internal types
type internal ProcessedType<'T> = ProcessedType of 'T
type internal PersistedType<'T> = PersistedType of 'T

type internal DataSetCountType = internal DataSetCountType of CountPrimitive
type internal DeltaCountType = internal DeltaCountType of CountPrimitive
type internal RunIdType = internal RunIdType of RunIdPrimitive
type internal SubscriptionDataSetIdType = internal SubscriptionDataSetIdType of SubscriptionDataSetIdPrimitive
type internal EntityIdentifierType = EntityIdentifierPrimitive

type internal DeltaSnapshotCacheRowActionType = | Insert | Update
type internal TransactionStartedState = TransactionStartedState
[<Struct;NoEquality;NoComparison>]
type internal DataSetRunType = { SubscriptionDataSetId: SubscriptionDataSetIdType; RunIdCurr: RunIdType; RunIdPrev: RunIdType option }

//////////////////////////////
// Public types 
[<Struct;NoComparison>]
type public DeltaStateType = | CUR | ADD | UPD | DEL with
    override this.ToString() = this |> Union.fromDuCaseToString
[<Struct;NoEquality;NoComparison>]
type public RunModeType = | SET_DELTA | SET_ALL (*| SET_RESET | ATC_ALL | ATC_DELTA | ATC_RESET*) with
    override this.ToString() = this |> Union.fromDuCaseToString

/// Interface of .NET data set entity that can be tracked for deltas
[<AllowNullLiteral>]
type public IDataSetEntity = 
    abstract member Identifier: EntityIdentifierPrimitive with get 

/// Interface of a subscription
type public ISubscription =
    abstract member SubscriptionDataSetId: SubscriptionDataSetIdPrimitive with get
    abstract member SubscriptionDataSetFilter: string with get

/// Record of delta snapshot (message) 
[<NoEquality;NoComparison>]
type public DeltaSnapshotMessage<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    { Id: EntityIdentifierType; Delta: DeltaStateType; Date: SnapshotDatePrimitive; IsFull: bool; Cur: 'TEntity; Prv: 'TEntity }

/// Immutable cache row. Primary key will have default value prior to insert. 
[<NoEquality;NoComparison>]
type public DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    {   PrimaryKey: 'TCachePrimaryKey; SubscriptionDataSetId: SubscriptionDataSetIdPrimitive; RunId: RunIdPrimitive; EntityIdentifier: EntityIdentifierPrimitive; 
        EntityDeltaCode: DeltaStatePrimitive; EntityDeltaDate: SnapshotDatePrimitive; EntityDataCurrent: 'TEntity; EntityDataPrevious: 'TEntity }

////////////////////////////////
// Internal types         
type internal PersistProcessedCacheRowFuncType<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    ProcessedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>> -> PersistedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>>
type internal InsertUpdateCacheType<'TCachePrimaryKey, 'TEntity  when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    { Insert: PersistProcessedCacheRowFuncType<'TCachePrimaryKey, 'TEntity>; Update: PersistProcessedCacheRowFuncType<'TCachePrimaryKey, 'TEntity> }
type internal DataSetProcessResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    { DataSetCount: DataSetCountType; DataSetEntityIds: EntityIdentifierType[]; DeltaSnapshotMessages: DeltaSnapshotMessage<'TEntity> seq }
type internal CacheRowPendingPersistence<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    { CacheRowProcessed: ProcessedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>>; CacheActionPending: DeltaSnapshotCacheRowActionType }

//////////////////////////////
// Public types 
/// Result of finding a cache row
[<Struct;NoEquality;NoComparison>]
type public FindCacheEntryResultType<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    internal | NotFoundCacheEntry | FoundCacheEntry of DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>
// Result of finding latest run id of data set
[<Struct;NoEquality;NoComparison>]
type public FindCacheLatestRunIdResultType = 
    internal | NotFoundRunId | FoundRunId of RunIdType
[<Struct;NoEquality;NoComparison>]
/// Strategy for handling an empty publisher data set during a Subscriber.GetDeltas(). Does not apply to Subscriber.GetDeltasAndCurrents call().
type public EmptyDataSetGetDeltasStrategyType = 
    /// Run will be considered successful, resuling in delete deltas being generated for all cache rows.
    | DefaultProcessing 
    /// Run will be considered successful, but deltas will not be generated and cache table transasction will be rolled back.
    | RunSuccessWithBypass 
    /// Run will fail and cache table transaction will be rolled back.
    | RunFailure 

/// Record of run result
[<NoEquality;NoComparison>]
type public RunResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    { IsSuccess: bool; RunId: RunIdPrimitive; ErrorMsgs: string seq; DeltaSnapshots: DeltaSnapshotMessage<'TEntity> seq; DataSetCount: CountPrimitive; DeltaCount: CountPrimitive }

// Delegates 
/// Determines whether two entities are equal by structure/value
type public IsEqualByValueDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
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
type public InsertCacheEntryDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> -> unit
/// Update row in cache
type public UpdateCacheEntryDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> -> unit

/// Find most recent cache entry row in subscription data set with entity identifier
type public FindLatestCacheEntryDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of SubscriptionDataSetIdPrimitive * EntityIdentifierType -> FindCacheEntryResultType<'TCachePrimaryKey, 'TEntity>
/// Retrieve all cache entry rows in subscription data set by run id excluding a specifc delta state
type public GetCacheEntryDataSetRunExcludeDeltaStateDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    delegate of SubscriptionDataSetIdPrimitive * RunIdPrimitive * DeltaStatePrimitive -> DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> seq

/// Find most recent cache row run id for a subsription data set id
type public FindCacheEntryLatestRunIdOfDataSetDelegate =
    delegate of SubscriptionDataSetIdPrimitive -> FindCacheLatestRunIdResultType

/// Record of all necessary cache operations
[<Struct;NoEquality;NoComparison>]
type public CacheEntryOperation<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    {   BeginTransaction: BeginTransactionDelegate;
        CommitTransaction: CommitTransactionDelegate;
        RollbackTransaction: RollbackTransactionDelegate;
        GetRunIdLatestOfDataSet: FindCacheEntryLatestRunIdOfDataSetDelegate;
        Insert: InsertCacheEntryDelegate<'TCachePrimaryKey, 'TEntity>; 
        Update: UpdateCacheEntryDelegate<'TCachePrimaryKey, 'TEntity>; 
        FindLatest: FindLatestCacheEntryDelegate<'TCachePrimaryKey, 'TEntity>; 
        GetDataSetRunExcludeDeltaState: GetCacheEntryDataSetRunExcludeDeltaStateDelegate<'TCachePrimaryKey, 'TEntity>; }