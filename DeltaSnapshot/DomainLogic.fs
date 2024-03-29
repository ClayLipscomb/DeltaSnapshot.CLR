﻿//------------------------------------------------------------------------------
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
module internal OptionUtil =
    let someIf isCreateSome value = if isCreateSome then Some value else None

[<AutoOpen>]
module internal Logging =
    let logMsg strOption = match strOption with | Some str -> printfn "%s" str | None -> ()
    let isLogging = true

module internal Processed =
    let create = ProcessedType 
    let value processedType = let (ProcessedType processedTypeValue) = processedType in processedTypeValue
module internal Persisted =
    let ofProcessed processedType = let (ProcessedType processedTypeValue) = processedType in processedTypeValue |> PersistedType
    let value persistedType = let (PersistedType persistedTypeValue) = persistedType in persistedTypeValue
module internal RunId =
    let create = RunIdType 
    let value runId = let (RunIdType runIdValue) = runId in runIdValue
module internal SubscriptionDataSetId =
    let create = SubscriptionDataSetIdType
    let value subscriptionDataSetId = let (SubscriptionDataSetIdType subscriptionDataSetIdValue) = subscriptionDataSetId in subscriptionDataSetIdValue
module internal DataSetCount =
    let create = DataSetCountType
    let value dataSetCount = let (DataSetCountType dataSetCountValue) = dataSetCount in dataSetCountValue
    let zero = DataSetCountType 0
    let add dataSetCount1 dataSetCount2 = (value dataSetCount1 + value dataSetCount2) |> create
module internal DeltaCount =
    let create = DeltaCountType
    let zero = DeltaCountType 0
module internal DeltaState =
    let private unionCache = Union.getUnionCases<DeltaStateType> |> Seq.cache 
    let fromStr delaStateCandidateStr = Union.createUnionCase<DeltaStateType> unionCache delaStateCandidateStr
module internal ProccessedCacheRowLogInfo = 
    let create (priorDeltaStateOption, isRowFound, isEqualsOption) = 
        { IsRowFound = isRowFound; IsEqualsOption = isEqualsOption; PriorDeltaStateOption = priorDeltaStateOption }

module internal CacheOperationBatch =
    let create<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (   beginTransaction, commitTransaction, rollbackTransaction, getRunIdLatestOfDataSet, 
                insert, update, findNewest, getDataSetRunExcludeDeltaState: GetDataSetRunEntityExcludeDeltaStateCacheDelegate<'TCachePrimaryKey, 'TEntity>) = 
        {   BeginTransaction = beginTransaction; CommitTransaction = commitTransaction; RollbackTransaction = rollbackTransaction; GetRunIdNewest = getRunIdLatestOfDataSet;
            Insert = insert; Update = update; FindNewest = findNewest; GetDataSetRunExcludeDeltaState = getDataSetRunExcludeDeltaState }

module internal CacheOperationEvent =
    let create<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (   beginTransaction, commitTransaction, rollbackTransaction, insert, lockOldest, findNewest: FindNewestDataSetEntityByIdCacheDelegate<'TCachePrimaryKey, 'TEntity>) = 
        {   BeginTransaction = beginTransaction; CommitTransaction = commitTransaction; RollbackTransaction = rollbackTransaction; Insert = insert; LockOldest = lockOldest; FindNewest = findNewest }

module internal CacheRowPendingPersistence =
    let create<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
        (cacheRow: DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>, cacheActionPending) logInfoOption = 
        let buildProcessMessage = 
            match logInfoOption with
            | Some logInfo ->
                $"run:{cacheRow.RunId}" 
                + " " + if logInfo.IsRowFound then "Exists" else "!Exist"
                + " " + if logInfo.IsRowFound then (match logInfo.PriorDeltaStateOption with | Some priorDeltaState -> $"{priorDeltaState}" | None -> "UNK") else replicate 3 @" "
                + " " + match logInfo.IsEqualsOption with | Some isEquals -> (if isEquals then "=="  else "!=") | None -> replicate 2 @" "
                + $" -> {cacheRow.EntityDeltaCode.ToString()} {cacheActionPending.ToString()} id:{cacheRow.EntityIdentifier}" |> Some
            | None -> None
        { CacheRowProcessed = Processed.create cacheRow; CacheActionPending = cacheActionPending; ProcessedMessage = buildProcessMessage }
    let logProcessedMessageAndReturn cacheRow = logMsg cacheRow.ProcessedMessage; cacheRow

module internal DeltaSnapshotCacheRow = // needs unit tests
    let private progress row (RunIdType runIdValue) = { row with RunId = runIdValue; EntityDeltaDate = DateTimeOffset.Now }
    let toAdd existingRow entityLatest =    // fromDelToAdd-I
        { existingRow with EntityDeltaCode = DeltaStateType.ADD.ToString(); EntityDataPrevious = null; EntityDataCurrent = entityLatest }
        |> progress 
    let toUpd existingRow entityLatest =    // fromCurToUpd-U, fromAddToUpd-I
        { existingRow with EntityDeltaCode = DeltaStateType.UPD.ToString(); EntityDataPrevious = existingRow.EntityDataCurrent; EntityDataCurrent = entityLatest }
        |> progress 
    let toDel existingRow =                 // fromCurToDel-U, fromAddToDel-I, fromUpdToDel-I
        { existingRow with EntityDeltaCode = DeltaStateType.DEL.ToString(); EntityDataPrevious = existingRow.EntityDataCurrent; EntityDataCurrent = null }
        |> progress 
    let toCur existingRow =                 // fromAddToCur-I, fromUpdToCur-I
        { existingRow with EntityDeltaCode = DeltaStateType.CUR.ToString(); EntityDataPrevious = null }
        |> progress 
    let createAdd (RunIdType runIdValue) (SubscriptionDataSetIdType subscriptionDataSetIdValue) entityLatest = // -I
        {   PrimaryKey = Unchecked.defaultof<'TCachePrimaryKey>; SubscriptionDataSetId = subscriptionDataSetIdValue; RunId = runIdValue; EntityDeltaDate = DateTimeOffset.Now;
            EntityDeltaCode = DeltaStateType.ADD.ToString();  EntityDataPrevious = null; EntityDataCurrent = entityLatest; EntityIdentifier = entityLatest.Identifier }

module internal DeltaSnapshotMessage = 
    let ofCacheRowPersisted runResultScope (PersistedType cacheRow) = 
        match (runResultScope, DeltaState.fromStr cacheRow.EntityDeltaCode) with
        | (DeltasOnly, None) | (All, None) -> None  // ignore invalid deltas
        | (All, Some deltaStateType) | (DeltasOnly, Some deltaStateType) ->
            match (runResultScope, deltaStateType) with 
            | (DeltasOnly, CUR) -> None             // do not return a CUR snapshot for deltas only
            | (DeltasOnly, ADD) | (DeltasOnly, DEL) | (DeltasOnly, UPD) | (All, ADD) | (All,  DEL) | (All, UPD) | (All, CUR) ->
                {   Id = cacheRow.EntityIdentifier; Delta = deltaStateType; IsFull = (match runResultScope with | DeltasOnly -> false | All -> true); 
                    Date = cacheRow.EntityDeltaDate; Cur = cacheRow.EntityDataCurrent; Prv = cacheRow.EntityDataPrevious } |> Some

module internal ProcessDataSetResult =
    let zero () = { DataSetCount = DataSetCount.zero; DataSetEntityIds = Array.empty; DeltaSnapshotMessages = Array.empty }
    let ofCacheRowPersisted runResultScope cacheRowPersisted = 
        {   DataSetCount = DataSetCount.create 1;
            DataSetEntityIds = Array.singleton (Persisted.value cacheRowPersisted).EntityIdentifier ;
            DeltaSnapshotMessages = match DeltaSnapshotMessage.ofCacheRowPersisted runResultScope cacheRowPersisted with | Some snapshotMsg -> Array.singleton snapshotMsg | None -> Array.empty }
    let add processDataSetResult1 processDataSetResult2 =   // monoid
        {   DataSetCount = DataSetCount.add processDataSetResult1.DataSetCount processDataSetResult2.DataSetCount;
            DataSetEntityIds = Array.append processDataSetResult1.DataSetEntityIds processDataSetResult2.DataSetEntityIds;
            DeltaSnapshotMessages = Array.append processDataSetResult1.DeltaSnapshotMessages processDataSetResult2.DeltaSnapshotMessages }
    let logCounts processDataSetResult =
            Logging.logMsg (Some (String.Format ("ProcessDataSetResult.DataSetCount: {0}", (DataSetCount.value processDataSetResult.DataSetCount))))
            Logging.logMsg (Some (String.Format ("ProcessDataSetResult.DataSetEntityIds.Length: {0}", (processDataSetResult.DataSetEntityIds.Length))))
            Logging.logMsg (Some (String.Format ("ProcessDataSetResult.DeltaSnapshotMessages.Length: {0}", (processDataSetResult.DeltaSnapshotMessages.Length))))

module internal RunResult =
    let createSuccess (RunIdType runIdValue, errorMessage, deltaSnapshots, DataSetCountType dataSetCountValue, DeltaCountType deltaCountValue) = 
        { IsSuccess = true; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = deltaSnapshots; DataSetCount = dataSetCountValue; DeltaCount = deltaCountValue }
    let createFailure (RunIdType runIdValue, errorMessage) = 
        { IsSuccess = false; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = Seq.empty; DataSetCount = 0; DeltaCount = 0 }

module internal IO = 
    let private execPullDataSet (pullPublisherDataSet: PullPublisherDataSetDelegate<'TEntity>, subscription: ISubscription) = 
        pullPublisherDataSet.Invoke (subscription)

    let private execBeginTransaction (beginTransactionDelegte: BeginTransactionDelegate) (dataSetRun: DataSetRunType) = 
        dataSetRun |> ignore    // parameter ensures a data set run has been instantiated
        do beginTransactionDelegte.Invoke ()
        TransactionStartedState
    let private execCommitAndReturn (commitTransactionDelegate: CommitTransactionDelegate) (result: RunResultType<'TEntity>) =
        do commitTransactionDelegate.Invoke ()
        result
    let private execRollbackAndReturn (rollbackTransactionDelegate: RollbackTransactionDelegate) (result: RunResultType<'TEntity>) =
        do rollbackTransactionDelegate.Invoke ()
        result
    let private execInsertCache<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
            (insert: InsertDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>) cacheEntryProcessed =
        insert.Invoke (Processed.value cacheEntryProcessed) |> ignore
        cacheEntryProcessed |> Persisted.ofProcessed
    let private execUpdateCache<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
            (update: UpdateDataSetEntityCacheDelegate<'TCachePrimaryKey, 'TEntity>) cacheEntryProcessed =
        do update.Invoke (Processed.value cacheEntryProcessed)
        cacheEntryProcessed |> Persisted.ofProcessed 
    let private execFindNewestCache<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
            (findNewestCacheEntry: FindNewestDataSetEntityByIdCacheDelegate<'TCachePrimaryKey, 'TEntity>) subscriptionDataSetId entityIdentifier = 
        match findNewestCacheEntry.Invoke (SubscriptionDataSetId.value subscriptionDataSetId, entityIdentifier) with
        | NotFoundCacheEntry -> None
        | FoundCacheEntry cacheEntry -> Some cacheEntry

    let private execGetNewestRunIdCache (findCacheNewestRunId: FindNewestRunIdOfDataSetCacheDelegate) subscriptionDataSetId =
        match findCacheNewestRunId.Invoke (SubscriptionDataSetId.value subscriptionDataSetId) with | NotFoundRunId -> None | FoundRunId runIdValue -> Some (RunId.create runIdValue)

    let private execGetPreviousNonDeletesCache (getByRunIdExcludeDeltaState: GetDataSetRunEntityExcludeDeltaStateCacheDelegate<'TCachePrimaryKey,'TEntity>) dataSetRun = 
        match dataSetRun.RunIdPrev with
        | None          -> Seq.empty
        | Some runId    -> 
            getByRunIdExcludeDeltaState.Invoke (SubscriptionDataSetId.value dataSetRun.SubscriptionDataSetId, RunId.value runId, DeltaStateType.DEL.ToString())
            |> Seq.filter(fun row -> (row.EntityDeltaCode |> DeltaState.fromStr) <> Some DeltaStateType.DEL)    // ensure result set has no deletes
    let private execGetRunNonCurrentsCache (getByRunIdExcludeDeltaState: GetDataSetRunEntityExcludeDeltaStateCacheDelegate<'TCachePrimaryKey,'TEntity>) subscriptionDataSetId runIdOption = 
        match runIdOption with 
        | Some runId ->
            getByRunIdExcludeDeltaState.Invoke (SubscriptionDataSetId.value subscriptionDataSetId, RunId.value runId, DeltaStateType.CUR.ToString())
            |> Seq.filter(fun row -> (row.EntityDeltaCode |> DeltaState.fromStr) <> Some DeltaStateType.CUR)    // ensure result set has no currents
        | None -> Seq.empty

    let private persistProcessedEntityCache (action :InsertUpdateCacheType<'TCachePrimaryKey, 'TEntity>) (processDataSetEntityTypeResult) : PersistedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>> = 
        processDataSetEntityTypeResult.CacheRowProcessed
        |> match processDataSetEntityTypeResult.CacheActionPending with | Insert -> action.Insert | Update -> action.Update

    let createFuncPullDataSet<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (pullPublisherDataSetDelegate: PullPublisherDataSetDelegate<'TEntity>, subscription) =
        fun () -> execPullDataSet (pullPublisherDataSetDelegate, subscription)

    let createFuncGetNewestRunId<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (cacheOperationBatch: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity>) =
        fun (subscriptionDataSetId) -> execGetNewestRunIdCache cacheOperationBatch.GetRunIdNewest subscriptionDataSetId
    let createFuncGetRunNonCurrent<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (cacheOperationBatch: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity>) subscriptiionDataSetId = 
        fun (runId) -> execGetRunNonCurrentsCache cacheOperationBatch.GetDataSetRunExcludeDeltaState subscriptiionDataSetId runId
    let createCacheFuncBatch<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (cacheOperationBatch: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity>) subscriptionDataSetId =
        (   fun (dataSetRun: DataSetRunType) -> execBeginTransaction cacheOperationBatch.BeginTransaction dataSetRun
            , fun (result: RunResultType<'TEntity>) -> execCommitAndReturn cacheOperationBatch.CommitTransaction result 
            , fun (result: RunResultType<'TEntity>) -> execRollbackAndReturn cacheOperationBatch.RollbackTransaction result 
            , createFuncGetNewestRunId cacheOperationBatch
            , fun (dataSetRun) -> execGetPreviousNonDeletesCache cacheOperationBatch.GetDataSetRunExcludeDeltaState dataSetRun
            , fun (entityIdentifer) -> execFindNewestCache cacheOperationBatch.FindNewest subscriptionDataSetId entityIdentifer
            , fun (cacheRowPendingPersistenceType: CacheRowPendingPersistenceType<'TCachePrimaryKey, 'TEntity>) ->
                persistProcessedEntityCache { Insert = execInsertCache cacheOperationBatch.Insert; Update = execUpdateCache cacheOperationBatch.Update } cacheRowPendingPersistenceType )

[<AutoOpen>]
module internal DeltaSnapshotCore = 
    let processDataSetEntity (subscriptionDataSetId, runId) (isEqual) (entity, cacheEntryFoundOption) =
        let someIfLogging = someIf isLogging
        match cacheEntryFoundOption with
        | None ->                                                           // Insert ADD
            CacheRowPendingPersistence.create (DeltaSnapshotCacheRow.createAdd runId subscriptionDataSetId entity, Insert) (ProccessedCacheRowLogInfo.create(None, false, None) |> someIfLogging)
        | Some cacheEntryFound -> 
            let toUpd = DeltaSnapshotCacheRow.toUpd cacheEntryFound entity runId
            let toCur = DeltaSnapshotCacheRow.toCur cacheEntryFound runId
            let toAdd = DeltaSnapshotCacheRow.toAdd cacheEntryFound entity runId
            let deltaStateOption = DeltaState.fromStr cacheEntryFound.EntityDeltaCode
            let isCacheAndEntityEqual = isEqual (cacheEntryFound.EntityDataCurrent, entity)
            let logInfoOption = let isRowFound = true in ProccessedCacheRowLogInfo.create (deltaStateOption, isRowFound, Some isCacheAndEntityEqual) |> someIfLogging
            match isCacheAndEntityEqual, deltaStateOption with
            | true, Some DEL | false, Some DEL                          ->  // Insert (re)ADD (prior delete)
                CacheRowPendingPersistence.create (toAdd, Insert) logInfoOption
            | false, Some UPD | false, Some ADD                         ->  // Insert UPD
                CacheRowPendingPersistence.create (toUpd, Insert) logInfoOption
            | false, Some CUR | false, None (* treat invalid as CUR *)  ->  // Update UPD
                CacheRowPendingPersistence.create (toUpd, Update) logInfoOption
            | true, Some UPD | true, Some ADD                           ->  // Insert CUR, no change
                CacheRowPendingPersistence.create (toCur, Insert) logInfoOption
            | true, Some CUR | true, None (* treat invalid as CUR *)    ->  // Update CUR, no change
                CacheRowPendingPersistence.create (toCur, Update) logInfoOption

    let private processDataSet (dataSetRun: DataSetRunType, dataSet, isEqual, persistProcessed, findLatestCache, runResultScope) (transactionStartedState: TransactionStartedStateType) =
        transactionStartedState |> ignore   // only used to confirm cache transaction begun
        let processPersistToResult = 
            (fun e -> processDataSetEntity (dataSetRun.SubscriptionDataSetId, dataSetRun.RunIdCurr) isEqual  (e, findLatestCache e.Identifier))
            >> if isLogging then CacheRowPendingPersistence.logProcessedMessageAndReturn else id
            >> persistProcessed 
            >> ProcessDataSetResult.ofCacheRowPersisted runResultScope 
        dataSet 
        |> Array.ofSeq  // array required for parallel
        |> (if isLogging then Array.map else Array.Parallel.map) processPersistToResult    // logging must map sequentially (instead of parallel) for distinct log messages
        |> Array.fold ProcessDataSetResult.add (ProcessDataSetResult.zero ())   // monoid
        |> fun processDataSetResult -> if isLogging then ProcessDataSetResult.logCounts processDataSetResult; processDataSetResult else processDataSetResult

    let processNonDeleteCacheEntryAsDelete runIdCurr cacheEntryNonDelete =
        let cacheRowDelete = (DeltaSnapshotCacheRow.toDel cacheEntryNonDelete runIdCurr)
        let deltaStateOption = DeltaState.fromStr cacheEntryNonDelete.EntityDeltaCode
        let logInfoOption = if isLogging then ProccessedCacheRowLogInfo.create (deltaStateOption, true, None) |> Some else None
        match deltaStateOption with 
        | Some DeltaStateType.UPD | Some DeltaStateType.ADD ->
            (CacheRowPendingPersistence.create (cacheRowDelete, Insert) logInfoOption) |> Some
        | Some DeltaStateType.CUR (* CUR converts to DEL *) ->
            (CacheRowPendingPersistence.create (cacheRowDelete, Update) logInfoOption) |> Some
        | None (* invalid code *) | Some DeltaStateType.DEL (* DEL shoud not be possible *) -> // ignore these
            None

    let private processCacheResidualForDeletes dataSetEntityIds dataSetRun persistProcessed runResultScope (nonDeletesPrevious : DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity> seq) =
        nonDeletesPrevious
        |> Seq.filter (fun cacheEntryNonDelete -> dataSetEntityIds |> (Array.exists (fun id -> id = cacheEntryNonDelete.EntityIdentifier)) |> not) // excludes entity found in pub data set
        |> Seq.toArray  // array required for parallel
        |> Array.Parallel.map (processNonDeleteCacheEntryAsDelete dataSetRun.RunIdCurr)
        |> Array.Parallel.choose id
        |>  if isLogging then fun a -> logMsg (Some (String.Format ("nonDeletesPrevious filtered count: {0}", a.Length))); a |> Array.map CacheRowPendingPersistence.logProcessedMessageAndReturn
            else Array.Parallel.map id 
        |> Array.Parallel.map (persistProcessed 
            >> (DeltaSnapshotMessage.ofCacheRowPersisted runResultScope))
        |> Array.Parallel.choose id

    let private generate<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
        (runResultScope: RunResultScopeType) (deltaSnapshotPattern: DeltaSnapshotPatternType) (subscription: ISubscription) (runId: RunIdType) (pullPublisherDataSetDelegate: PullPublisherDataSetDelegate<'TEntity>) 
        (emptyDataSetGetDeltasStrategy: EmptyPublisherDataSetGetDeltasStrategyType) (isEqualByValue: IsEqualByValueDelegate<'TEntity>) (cacheOperationBatch: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity>) =        

        // initializations
        let subscriptionDataSetId = SubscriptionDataSetId.create subscription.SubscriptionDataSetId
        do logMsg (if isLogging then String.Format ("generate {0} {1}", deltaSnapshotPattern.ToString(), runResultScope.ToString()) |> Some else None)        

        // prepare IO and misc functions
        let (beginTransaction, commitAndReturn, rollbackAndReturn, getNewestRunId, getNonDeletesPrevious, findNewestCacheEntity, persistProcessed) = 
            IO.createCacheFuncBatch cacheOperationBatch subscriptionDataSetId
        let pullDataSet = IO.createFuncPullDataSet (pullPublisherDataSetDelegate, subscription)
        let isEqual = fun (entity1, entity2) -> isEqualByValue.Invoke (entity1, entity2)

        try
            let dataSetRun = { SubscriptionDataSetId = subscriptionDataSetId; RunIdCurr = runId; RunIdPrev = getNewestRunId subscriptionDataSetId }
            do logMsg (if isLogging then "runIdPrev:" + (match dataSetRun.RunIdPrev with | Some runId -> String.Format ("{0}", runId |> RunId.value) | None -> nameof None) |> Some else None)

            let processDataSetResult = 
                dataSetRun
                |> beginTransaction
                |> processDataSet (dataSetRun, pullDataSet (), isEqual, persistProcessed, findNewestCacheEntity, runResultScope) 

            match deltaSnapshotPattern with
            | Event ->
                let deltaSnapshots = processDataSetResult.DeltaSnapshotMessages
                commitAndReturn (RunResult.createSuccess (runId, @"Success.", deltaSnapshots, processDataSetResult.DataSetCount, DeltaCount.create deltaSnapshots.Length ))
            | Batch ->
                match (runResultScope = DeltasOnly && processDataSetResult.DataSetCount = DataSetCount.zero, emptyDataSetGetDeltasStrategy) with
                    | false, EmptyPublisherDataSetGetDeltasStrategyType.RunFailure 
                    | false, EmptyPublisherDataSetGetDeltasStrategyType.RunSuccessWithBypass
                    | false, EmptyPublisherDataSetGetDeltasStrategyType.DefaultProcessingDeleteAll 
                    | true,  EmptyPublisherDataSetGetDeltasStrategyType.DefaultProcessingDeleteAll ->
                        let nonDeletesPrevious = getNonDeletesPrevious dataSetRun
                        let deltaSnapshots = 
                            processCacheResidualForDeletes processDataSetResult.DataSetEntityIds dataSetRun persistProcessed runResultScope nonDeletesPrevious 
                            |> Array.append <| processDataSetResult.DeltaSnapshotMessages
                        commitAndReturn (RunResult.createSuccess (runId, @"Success.", deltaSnapshots, processDataSetResult.DataSetCount, DeltaCount.create deltaSnapshots.Length))
                    | true,  EmptyPublisherDataSetGetDeltasStrategyType.RunFailure -> 
                        rollbackAndReturn (RunResult.createFailure (runId, @"Publisher data set is empty."))
                    | true,  EmptyPublisherDataSetGetDeltasStrategyType.RunSuccessWithBypass ->
                        rollbackAndReturn (RunResult.createSuccess (runId, @"Run bypassed due to empty publisher data set.", Seq.empty, DataSetCount.zero, DeltaCount.zero))
        with
            | _ as ex -> 
                do logMsg ( String.Format ("Unexpected exception {0} {1} {2} {3} {4}", (RunId.value runId), deltaSnapshotPattern, runResultScope.ToString(), ex.Message, ex.StackTrace) |> Some)
                rollbackAndReturn (RunResult.createFailure (runId, ex.Message))

    let private pullBatchDeltasLastRun<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (subscription: ISubscription) (cacheOperationBatch: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity>) = 
        do logMsg (if isLogging then String.Format ("pullBatchDeltasLastRun {0}", DateTimeOffset.Now.ToString()) |> Some else None) 
        
        let subscriptionDataSetId = SubscriptionDataSetId.create subscription.SubscriptionDataSetId
        let (getNewestRunId, getRunNonCurrents) = (IO.createFuncGetNewestRunId cacheOperationBatch, IO.createFuncGetRunNonCurrent cacheOperationBatch subscriptionDataSetId)
        (getNewestRunId subscriptionDataSetId) |> getRunNonCurrents 

    let pullBatchDeltas subscription = generate RunResultScopeType.DeltasOnly Batch subscription
    let pullBatchDeltasAndCurrents subscription = generate RunResultScopeType.All Batch subscription 

    let getEventDelta subscription = generate RunResultScopeType.DeltasOnly Event subscription