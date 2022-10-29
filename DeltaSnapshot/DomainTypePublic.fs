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
type internal SubscriptionDataSetIdPrimitive = Int64
type internal EntityIdentifierPrimitive = string
type internal SnapshotDatePrimitive = DateTimeOffset
type internal CountPrimitive = Int32
type internal DeltaStatePrimitive = string

/// Interface of .NET data set entity that can be tracked for deltas.
[<AllowNullLiteral>]
type public IDataSetEntity = 
    /// Uniquely identifies entity in data set
    abstract member Identifier: EntityIdentifierPrimitive with get 

[<Struct;NoComparison>]
type public DeltaStateType = 
    /// Current (no change)
    //| CUR 
    /// Add (new row)
    | ADD 
    /// Update (row has changed)
    | UPD 
    /// Delete (row has been removed from data set)
    | DEL with
    override this.ToString() = this |> Union.fromDuCaseToString

/// Immutable snapshot message.
[<NoEquality;NoComparison>]
type public DeltaSnapshotMessage<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = {   
        /// Unique identifer of entity
        Id: EntityIdentifierPrimitive; 
        /// Delta state of snapshot
        Delta: DeltaStateType; 
        /// Date snapshot was created
        Date: SnapshotDatePrimitive; 
        /// Is this from a pull of deltas and currents? (not deltas only)
        IsFull: bool; 
        /// Current version of entity
        Cur: 'TEntity; 
        /// Previous version of entity
        Prv: 'TEntity }

/// Immutable cache row. Prior to insert the primary key will have its default value and must be set. 
[<NoEquality;NoComparison>]
type public DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = {   
        /// Primary key value of snapshot row in cache table
        PrimaryKey: 'TCachePrimaryKey; 
        /// Uniquely identifies data set of the subscriber/subscription
        SubscriptionDataSetId: SubscriptionDataSetIdPrimitive; 
        /// Identifes executed snapshot run
        RunId: RunIdPrimitive; 
        /// Unique identifer of entity
        EntityIdentifier: EntityIdentifierPrimitive; 
        /// Delta state of snapshot
        EntityDeltaCode: DeltaStatePrimitive; 
        /// Date snapshot was created
        EntityDeltaDate: SnapshotDatePrimitive; 
        /// Current version of entity
        EntityDataCurrent: 'TEntity; 
        /// Previous version of entity
        EntityDataPrevious: 'TEntity }

/// Interface of a subscription
type public ISubscription =
    /// Uniquely identifies data set of the subscriber/subscription
    abstract member SubscriptionDataSetId: SubscriptionDataSetIdPrimitive with get
    /// Subscription-specific serialized filter to be applied to publisher data set (optional)
    abstract member SubscriptionDataSetFilter: string with get

/// Result of finding a cache row
[<Struct;NoEquality;NoComparison>]
type public FindCacheEntryResultType<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    internal | NotFoundCacheEntry | FoundCacheEntry of DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>

// Result of finding latest run id of data set
[<Struct;NoEquality;NoComparison>]
type public FindCacheNewestRunIdResultType = 
    internal | NotFoundRunId | FoundRunId of RunIdPrimitive

[<Struct;NoEquality;NoComparison>]
/// Strategy for handling an empty publisher data set during a Subscriber.GetDeltas(). Does not apply to Subscriber.GetDeltasAndCurrents call().
type public EmptyPublisherDataSetGetDeltasStrategyType = 
    /// Run will be considered successful, resuling in delete deltas being generated for all cached rows.
    | DefaultProcessingDeleteAll 
    /// Run will be considered "successful", but deltas will not be generated and cache table transaction will be rolled back..
    | RunSuccessWithBypass 
    /// Run will fail and cache table transaction will be rolled back.
    | RunFailure 

[<Struct;NoEquality;NoComparison>]
/// Event strategy for locking the subscription data set cache for a specific entity
type public EventCacheLockingStrategyType = 
    /// No cache locking will occur. Only advisable for low volume event pushes by publisher.
    | BypassCacheLocking 
    /// If a lock attempt fails or times out, proceed with processing as though lock was successful.
    | CacheLockingProceedIfLockFailure
    /// If a lock attempt fails or times out, notify consumer of failure instead of proceeding with processing.
    | CacheLockingNotifyConsumerIfLockFailure

/// Result of a run returned to subscriber
[<NoEquality;NoComparison>]
type public RunResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = { 
    /// Was run successful?
    IsSuccess: bool; 
    /// Identifes executed snapshot run
    RunId: RunIdPrimitive; 
    /// Collection of error messages if run failure
    ErrorMsgs: string seq; 
    /// Collection of snapshot messages from run
    DeltaSnapshots: DeltaSnapshotMessage<'TEntity> seq; 
    /// Number of rows in publisher data set at time of run.
    DataSetCount: CountPrimitive; 
    /// Number of deltas (ADD, UPD, DEL) found during run
    DeltaCount: CountPrimitive }

/// Determines whether two entities are equal by structure/value
type public IsEqualByValueDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of 'TEntity * 'TEntity -> bool
/// Retrieve entire publisher data set (batch only)
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
/// Insert row into cache and return primary key
type public InsertDataSetEntityCacheDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> -> 'TCachePrimaryKey
/// Update row in cache
type public UpdateDataSetEntityCacheDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> -> unit
/// Lock oldest cache entry row in subscription data set with entity identifier
type public LockOldestDataSetEntityByIdCacheDelegate<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of SubscriptionDataSetIdPrimitive * EntityIdentifierPrimitive -> bool
/// Find most recent cache entry row in subscription data set with entity identifier
type public FindNewestDataSetEntityByIdCacheDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
    delegate of SubscriptionDataSetIdPrimitive * EntityIdentifierPrimitive -> FindCacheEntryResultType<'TCachePrimaryKey, 'TEntity>
/// Retrieve all cache entry rows in subscription data set by run id 
type public GetDataSetRunCacheDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    delegate of SubscriptionDataSetIdPrimitive * RunIdPrimitive -> DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> seq
/// Retrieve all cache entry rows in subscription data set by run id excluding a specifc delta state
type public GetDataSetRunExcludeDeltaStateCacheDelegate<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> =
    delegate of SubscriptionDataSetIdPrimitive * RunIdPrimitive * DeltaStatePrimitive -> DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> seq
/// Find most recent cache row run id for a subsription data set id
type public FindNewestRunIdOfDataSetCacheDelegate =
    delegate of SubscriptionDataSetIdPrimitive -> FindCacheNewestRunIdResultType

/// All cache operations required for batch pattern
[<Struct;NoEquality;NoComparison>]
type public CacheOperationBatchType<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = {   
        BeginTransaction: BeginTransactionDelegate
        CommitTransaction: CommitTransactionDelegate
        RollbackTransaction: RollbackTransactionDelegate
        GetRunIdNewest: FindNewestRunIdOfDataSetCacheDelegate
        Insert: InsertDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>
        Update: UpdateDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>
        FindNewest: FindNewestDataSetEntityByIdCacheDelegate<'TCachePrimaryKey, 'TEntity>
        GetDataSetRunExcludeDeltaState: GetDataSetRunExcludeDeltaStateCacheDelegate<'TCachePrimaryKey, 'TEntity> }

/// All cache operations required for event pattern
[<Struct;NoEquality;NoComparison>]
type public CacheOperationEventType<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = {   
        BeginTransaction: BeginTransactionDelegate
        CommitTransaction: CommitTransactionDelegate
        RollbackTransaction: RollbackTransactionDelegate
        Insert: InsertDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>
        LockOldest: LockOldestDataSetEntityByIdCacheDelegate<'TEntity> 
        FindNewest: FindNewestDataSetEntityByIdCacheDelegate<'TCachePrimaryKey, 'TEntity> }  