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
module internal OptionUtil =
    let someIf isCreateSome value = if isCreateSome then Some value else None

[<AutoOpen>]
module internal Logging =
    let logMsg strOption = match strOption with | Some str -> printfn "%s" str | None -> ()

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
    let zero = DataSetCountType 0
module internal DeltaCount =
    let create = DeltaCountType
    let zero = DeltaCountType 0
module internal DeltaState =
    let private unionCache = Union.getUnionCases<DeltaStateType> |> Seq.cache 
    let fromStr delaStateCandidateStr = Union.createUnionCase<DeltaStateType> unionCache delaStateCandidateStr
module internal ProccessedCacheRowLogInfo = 
    let create priorDeltaStateOption isRowFound isEqualsOption = 
        { IsRowFound = isRowFound; IsEqualsOption = isEqualsOption; PriorDeltaStateOption = priorDeltaStateOption }

module internal CacheOperationBatch =
    let create<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
        (   beginTransaction, commitTransaction, rollbackTransaction, getRunIdLatestOfDataSet, insert, update, findNewest, 
            getDataSetRunExcludeDeltaState: GetCacheDataSetRunEntityExcludeDeltaStateDelegate<'TCachePrimaryKey, 'TEntity>) = 
        {   BeginTransaction = beginTransaction; CommitTransaction = commitTransaction; RollbackTransaction = rollbackTransaction; GetRunIdLatestOfDataSet = getRunIdLatestOfDataSet;
            Insert = insert; Update = update; FindNewest = findNewest; GetDataSetRunExcludeDeltaState = getDataSetRunExcludeDeltaState }

module internal CacheOperationEvent =
    let create<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
        (   beginTransaction, commitTransaction, rollbackTransaction, insert, lock, findNewest: FindNewestCacheDataSetEntityByIdDelegate<'TCachePrimaryKey, 'TEntity>) = 
        {   BeginTransaction = beginTransaction; CommitTransaction = commitTransaction; RollbackTransaction = rollbackTransaction; Insert = insert; Lock = lock; FindNewest = findNewest }

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

module internal DeltaSnapshotMessage = 
    let ofDeltaSnapshotCacheRowPersisted runResultScope (PersistedType cacheRow) = 
        match (runResultScope, DeltaState.fromStr cacheRow.EntityDeltaCode) with
        | (DeltasOnly, Some CUR)                // will not return a CUR snapshot if it is deltas only
        | (DeltasOnly, None) | (All, None) ->   // ignore invalid deltas
            None
        | (DeltasOnly, Some ADD) | (DeltasOnly,  Some DEL) | (DeltasOnly, Some UPD)
        | (All, Some ADD) | (All,  Some DEL) | (All, Some UPD) | (All, Some CUR) ->
            {   Id = cacheRow.EntityIdentifier; Delta = cacheRow.EntityDeltaCode |> DeltaState.fromStr |> Option.defaultValue CUR;
                IsFull = (match runResultScope with | DeltasOnly -> false | All -> true); Date = cacheRow.EntityDeltaDate; Cur = cacheRow.EntityDataCurrent; Prv = cacheRow.EntityDataPrevious } 
            |> Some

module internal DeltaSnapshotCacheRow =
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

module internal RunResult =
    let createSuccess (RunIdType runIdValue, errorMessage, deltaSnapshots, DataSetCountType dataSetCountValue, DeltaCountType deltaCountValue) = 
        { IsSuccess = true; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = deltaSnapshots; DataSetCount = dataSetCountValue; DeltaCount = deltaCountValue }
    let createFailure (RunIdType runIdValue, errorMessage) = 
        { IsSuccess = false; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = Seq.empty; DataSetCount = 0; DeltaCount = 0 }

module internal IO = 
    let execPullDataSet (pullPublisherDataSet: PullPublisherDataSetDelegate<'TEntity>, subscription: ISubscription) = 
        pullPublisherDataSet.Invoke (subscription)
    let execBeginTransaction (beginTransactionDelegte: BeginTransactionDelegate) (dataSetRun: DataSetRunType) = 
        dataSetRun |> ignore
        do beginTransactionDelegte.Invoke ()
        TransactionStartedState
    let execCommitAndReturn (commitTransactionDelegate: CommitTransactionDelegate) (result: RunResultType<'TEntity>) =
        do commitTransactionDelegate.Invoke ()
        result
    let execRollbackAndReturn (rollbackTransactionDelegate: RollbackTransactionDelegate) (result: RunResultType<'TEntity>) =
        do rollbackTransactionDelegate.Invoke ()
        result
    let execGetLatestRunIdCache (findCacheNewestRunId: FindNewestCacheRunIdOfDataSetDelegate) subscriptionDataSetId =
        match findCacheNewestRunId.Invoke (SubscriptionDataSetId.value subscriptionDataSetId) with | NotFoundRunId -> None | FoundRunId runId -> Some runId
    let execGetPreviousNonDeletesCacheAsArray (getByRunIdExcludeDeltaState: GetCacheDataSetRunEntityExcludeDeltaStateDelegate<'TCachePrimaryKey,'TEntity>) dataSetRun = 
        match dataSetRun.RunIdPrev with
        | None          -> Seq.empty
        | Some runId    -> getByRunIdExcludeDeltaState.Invoke (SubscriptionDataSetId.value dataSetRun.SubscriptionDataSetId, RunId.value runId, DeltaStateType.DEL.ToString())
        |> Seq.filter(fun row -> (row.EntityDeltaCode |> DeltaState.fromStr) <> Some DeltaStateType.DEL)    // ensure result set has no deletes
        |> Array.ofSeq
    let execFindNewestCacheEntry<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
            (findNewestCacheEntry: FindNewestCacheDataSetEntityByIdDelegate<'TCachePrimaryKey, 'TEntity>) subscriptionDataSetId entityIdentifier = 
        match findNewestCacheEntry.Invoke (SubscriptionDataSetId.value subscriptionDataSetId, entityIdentifier) with
        | NotFoundCacheEntry -> None
        | FoundCacheEntry cacheEntry -> Some cacheEntry
    let execInsertCache<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
            (insert: InsertCacheDataSetEntityDelegate<'TCachePrimaryKey, 'TEntity>) cacheEntryProcessed =
        insert.Invoke (Processed.value cacheEntryProcessed) |> ignore
        cacheEntryProcessed |> Persisted.ofProcessed
    let execUpdateCache<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
            (update: UpdateCacheDataSetEntityDelegate<'TCachePrimaryKey, 'TEntity>) cacheEntryProcessed =
        do update.Invoke (Processed.value cacheEntryProcessed)
        cacheEntryProcessed |> Persisted.ofProcessed 
    let persistProcessedDataSetEntity (action :InsertUpdateCacheType<'TCachePrimaryKey, 'TEntity>) (processDataSetEntityTypeResult) : PersistedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>> = 
        processDataSetEntityTypeResult.CacheRowProcessed
        |> match processDataSetEntityTypeResult.CacheActionPending with | Insert -> action.Insert | Update -> action.Update
    let createFuncPullDataSet<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (pullPublisherDataSetDelegate: PullPublisherDataSetDelegate<'TEntity>, subscription) =
        fun () -> execPullDataSet (pullPublisherDataSetDelegate, subscription)
    let createCacheFuncBatch<'TCachePrimaryKey,'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (cacheOperationBatch: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity>) subscriptionDataSetId =
        (   fun (dataSetRun: DataSetRunType) -> execBeginTransaction cacheOperationBatch.BeginTransaction dataSetRun
            , fun (result: RunResultType<'TEntity>) -> execCommitAndReturn cacheOperationBatch.CommitTransaction result 
            , fun (result: RunResultType<'TEntity>) -> execRollbackAndReturn cacheOperationBatch.RollbackTransaction result 
            , fun (subscriptionDataSetId) -> execGetLatestRunIdCache cacheOperationBatch.GetRunIdLatestOfDataSet subscriptionDataSetId
            , fun (dataSetRun) -> execGetPreviousNonDeletesCacheAsArray cacheOperationBatch.GetDataSetRunExcludeDeltaState dataSetRun
            , fun (entityIdentifer) -> execFindNewestCacheEntry cacheOperationBatch.FindNewest subscriptionDataSetId entityIdentifer
            , fun (cacheRowPendingPersistenceType: CacheRowPendingPersistenceType<'TCachePrimaryKey, 'TEntity>) ->
                persistProcessedDataSetEntity { Insert = execInsertCache cacheOperationBatch.Insert; Update = execUpdateCache cacheOperationBatch.Update } cacheRowPendingPersistenceType )

[<AutoOpen>]
module internal DeltaSnapshotCore = 
    let processDataSetEntity (subscriptionDataSetId, runId) (isEqual) (isLogging: bool) (entity, cacheEntryFoundOption) =
        let someIfLogging = someIf isLogging
        match cacheEntryFoundOption with
        | None -> CacheRowPendingPersistence.create                         // fresh add
                    (DeltaSnapshotCacheRow.createAdd runId subscriptionDataSetId entity, Insert) (someIfLogging (ProccessedCacheRowLogInfo.create None false None)) 
        | Some cacheEntryFound -> 
            let toUpd = DeltaSnapshotCacheRow.toUpd cacheEntryFound entity runId
            let toCur = DeltaSnapshotCacheRow.toCur cacheEntryFound runId
            let toAdd = DeltaSnapshotCacheRow.toAdd cacheEntryFound entity runId
            let deltaStateOption = DeltaState.fromStr cacheEntryFound.EntityDeltaCode
            let isCacheAndEntityEqual = isEqual (cacheEntryFound.EntityDataCurrent, entity)
            let logInfoOption = let isRowFound = true in (someIfLogging (ProccessedCacheRowLogInfo.create deltaStateOption isRowFound (Some isCacheAndEntityEqual)))
            match isCacheAndEntityEqual, deltaStateOption with
            | true, Some DEL | false, Some DEL                          -> // Insert (re)ADD (prior delete)
                CacheRowPendingPersistence.create (toAdd, Insert) logInfoOption
            | false, Some UPD | false, Some ADD                         -> // Insert UPD
                CacheRowPendingPersistence.create (toUpd, Insert) logInfoOption
            | false, Some CUR | false, None (* treat invalid as CUR *)  -> // Update UPD
                CacheRowPendingPersistence.create (toUpd, Update) logInfoOption
            | true, Some UPD | true, Some ADD                           -> // Insert CUR, no change
                CacheRowPendingPersistence.create (toCur, Insert) logInfoOption
            | true, Some CUR | true, None (* treat invalid as CUR *)    -> // Update CUR, no change,
                CacheRowPendingPersistence.create (toCur, Update) logInfoOption

    let processDataSet (dataSetRun: DataSetRunType, dataSet, isEqual, persistProcessed, findLatestCache, runResultScope) isLogging (transactionStartedState: TransactionStartedState) =
        transactionStartedState |> ignore
        let processEntityForDataSetRun = processDataSetEntity (dataSetRun.SubscriptionDataSetId, dataSetRun.RunIdCurr) isEqual isLogging
        let nonDeleteDeltaSnapshotsGross = 
            dataSet |> Array.ofSeq
            |> Array.Parallel.map (fun entity -> processEntityForDataSetRun (entity, findLatestCache entity.Identifier)) 
            |> if isLogging then Array.map (fun c -> logMsg c.ProcessedMessage; c) else Array.Parallel.map (id) 
            |> Array.Parallel.map persistProcessed
            |> Array.Parallel.map (fun cacheRowPersisted -> (cacheRowPersisted |> Persisted.value).EntityIdentifier, DeltaSnapshotMessage.ofDeltaSnapshotCacheRowPersisted runResultScope cacheRowPersisted)
        {   DataSetCount = DataSetCount.create nonDeleteDeltaSnapshotsGross.Length;
            DataSetEntityIds = nonDeleteDeltaSnapshotsGross |> Array.map (fun (entityId, _) -> entityId);
            DeltaSnapshotMessages = nonDeleteDeltaSnapshotsGross |> Seq.choose (fun (_, entityOpt) -> entityOpt) } // filter out None (occurs for isFull only) and descontruct Some

    let processNonDeleteCacheEntryAsDelete runIdCurr isLogging cacheEntryNonDelete =
        let cacheRowDelete = (DeltaSnapshotCacheRow.toDel cacheEntryNonDelete runIdCurr)
        let deltaStateOption = DeltaState.fromStr cacheEntryNonDelete.EntityDeltaCode
        let logInfoOption = if isLogging then Some (ProccessedCacheRowLogInfo.create deltaStateOption true None) else None
        match deltaStateOption with 
        | Some DeltaStateType.UPD | Some DeltaStateType.ADD ->
            (CacheRowPendingPersistence.create (cacheRowDelete, Insert) logInfoOption) |> Some
        | Some DeltaStateType.CUR (* CUR converts to DEL *) ->
            (CacheRowPendingPersistence.create (cacheRowDelete, Update) logInfoOption) |> Some
        | None (* invalid *) | Some DeltaStateType.DEL (* DEL shoud not be possible *) -> // ignore these
            None

    let processCacheResidualForDeletes dataSetEntityIds dataSetRun persistProcessed runResultScope (nonDeletesPrevious : DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>[]) isLogging =
        do logMsg (if isLogging then String.Format ("cacheNonDeletes count: {0}", nonDeletesPrevious.Length) |> Some else None)
        nonDeletesPrevious
            |> Array.filter (fun cacheEntryNonDelete -> dataSetEntityIds |> (Array.exists (fun id -> id = cacheEntryNonDelete.EntityIdentifier)) |> not) 
            |> Array.Parallel.map (processNonDeleteCacheEntryAsDelete dataSetRun.RunIdCurr isLogging)
            |> Array.choose id
            |> if isLogging then Array.map (fun c -> logMsg c.ProcessedMessage; c) else Array.Parallel.map (id) 
            |> Array.Parallel.map persistProcessed
            |> Array.Parallel.map (DeltaSnapshotMessage.ofDeltaSnapshotCacheRowPersisted runResultScope)
            |> Array.choose id

    let private buildSnapshots<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
        (runResultScope: RunResultScopeType) (deltaSnapshotPattern: DeltaSnapshotPatternType) (subscription: ISubscription) (runId: RunIdType) (pullPublisherDataSetDelegate: PullPublisherDataSetDelegate<'TEntity>) 
        (emptyDataSetGetDeltasStrategy: EmptyPublisherDataSetGetDeltasStrategyType) (isEqualByValue: IsEqualByValueDelegate<'TEntity>) (cacheOperationBatch: CacheOperationBatchType<'TCachePrimaryKey, 'TEntity>) =        

        // initializations
        let isLogging = true
        let subscriptionDataSetId = SubscriptionDataSetId.create subscription.SubscriptionDataSetId
        do logMsg (if isLogging then String.Format ("buildSnapshots {0} {1} {2}", deltaSnapshotPattern.ToString(), runResultScope.ToString(), DateTimeOffset.Now.ToString()) |> Some else None)        

        // prepare IO and misc functions
        let (beginTransaction, commitAndReturn, rollbackAndReturn, getLatestRunId, getNonDeletesPreviousAsArray, findLatestCache, persistProcessed) = 
            IO.createCacheFuncBatch cacheOperationBatch subscriptionDataSetId
        let pullDataSet = IO.createFuncPullDataSet (pullPublisherDataSetDelegate, subscription)
        let isEqual = fun (entity1, entity2) -> isEqualByValue.Invoke (entity1, entity2)

        try
            let dataSetRun = { SubscriptionDataSetId = subscriptionDataSetId; RunIdCurr = runId; RunIdPrev = getLatestRunId subscriptionDataSetId }
            do logMsg (if isLogging then (match dataSetRun.RunIdPrev with | Some runId -> "runIdPrev:" + String.Format ("{0}", runId |> RunId.value) | None -> nameof None) |> Some else None)

            let processDataSetResult = 
                dataSetRun
                |> beginTransaction
                |> processDataSet (dataSetRun, pullDataSet (), isEqual, persistProcessed, findLatestCache, runResultScope) isLogging

            match deltaSnapshotPattern with
            | Event ->
                let deltaSnapshots = processDataSetResult.DeltaSnapshotMessages |> Array.ofSeq;
                commitAndReturn (RunResult.createSuccess (runId, @"Success.", deltaSnapshots, processDataSetResult.DataSetCount, DeltaCount.create deltaSnapshots.Length ))
            | Batch ->
                match (runResultScope = DeltasOnly && processDataSetResult.DataSetCount = DataSetCount.zero, emptyDataSetGetDeltasStrategy) with
                    | false, EmptyPublisherDataSetGetDeltasStrategyType.RunFailure 
                    | false, EmptyPublisherDataSetGetDeltasStrategyType.RunSuccessWithBypass
                    | false, EmptyPublisherDataSetGetDeltasStrategyType.DefaultProcessingDeleteAll 
                    | true,  EmptyPublisherDataSetGetDeltasStrategyType.DefaultProcessingDeleteAll ->
                        let nonDeletesPrevious = getNonDeletesPreviousAsArray dataSetRun
                        let deltaSnapshots = 
                            processCacheResidualForDeletes processDataSetResult.DataSetEntityIds dataSetRun persistProcessed runResultScope nonDeletesPrevious isLogging
                            |> Seq.append <| processDataSetResult.DeltaSnapshotMessages
                            |> Array.ofSeq;      
                        commitAndReturn (RunResult.createSuccess (runId, @"Success.", deltaSnapshots, processDataSetResult.DataSetCount, DeltaCount.create deltaSnapshots.Length))
                    | true,  EmptyPublisherDataSetGetDeltasStrategyType.RunFailure -> 
                        rollbackAndReturn (RunResult.createFailure (runId, @"Publisher data set is empty."))
                    | true,  EmptyPublisherDataSetGetDeltasStrategyType.RunSuccessWithBypass ->
                        rollbackAndReturn (RunResult.createSuccess (runId, @"Run bypassed due to empty publisher data set.", Seq.empty, DataSetCount.zero, DeltaCount.zero))
        with
            | _ as ex -> 
                do logMsg ( String.Format ($"Unexpected exception {0} {1} {2} {3} {4}", (RunId.value runId), deltaSnapshotPattern, runResultScope.ToString(), ex.Message, ex.StackTrace) |> Some)
                rollbackAndReturn (RunResult.createFailure (runId, ex.Message))

    let pullDeltasBatch subscription = buildSnapshots RunResultScopeType.DeltasOnly Batch subscription
    let pullDeltasAndCurrentsBatch subscription = buildSnapshots RunResultScopeType.All Batch subscription 

    let getDeltaEvent subscription = buildSnapshots RunResultScopeType.DeltasOnly Event subscription