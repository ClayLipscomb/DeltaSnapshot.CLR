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
module internal Debug =
#if DEBUG    
    let printfnDebug = printfn  //let printfnDebug2 (format:Printf.TextWriterFormat<'a>) :'a = 'a
#else
    let printfnDebug = printfn  
#endif

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

module internal DeltaSnapshotMessage = 
    let ofDeltaSnapshotCacheRowPersisted isFull (PersistedType cacheRow) = 
        match (isFull, DeltaState.fromStr cacheRow.EntityDeltaCode) with
        | (false, Some CUR) | (false, None) | (true, None) -> 
            None    // do not return a CUR snapshot if is not a full
        | (false, Some ADD) | (false,  Some DEL) | (false, Some UPD)
        | (true, Some ADD) | (true,  Some DEL) | (true, Some UPD) | (true, Some CUR) ->
            {   Id = cacheRow.EntityIdentifier; Delta = cacheRow.EntityDeltaCode |> DeltaState.fromStr |> Option.defaultValue CUR;
                IsFull = isFull; Date = cacheRow.EntityDeltaDate; Cur = cacheRow.EntityDataCurrent; Prv = cacheRow.EntityDataPrevious } |> Some

module internal RunResult =
    let createSuccess (RunIdType runIdValue, errorMessage, deltaSnapshots, DataSetCountType dataSetCountValue, DeltaCountType deltaCountValue) = 
        { IsSuccess = true; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = deltaSnapshots; DataSetCount = dataSetCountValue; DeltaCount = deltaCountValue }
    let createFailure (RunIdType runIdValue, errorMessage) = 
        { IsSuccess = false; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = Seq.empty; DataSetCount = 0; DeltaCount = 0 }

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
    let execGetLatestRunIdCache (findCacheLatestRunId: FindCacheEntryLatestRunIdOfDataSetDelegate) subscriptionDataSetId =
        match findCacheLatestRunId.Invoke (SubscriptionDataSetId.value subscriptionDataSetId) with
        | NotFoundRunId -> printfnDebug "runIdPrev not found"; None
        | FoundRunId runId -> printfnDebug "runIdPrev %i" (RunId.value runId); Some runId
    let execGetPreviousNonDeletesCacheAsArray (getByRunIdExcludeDeltaState: GetCacheEntryDataSetRunExcludeDeltaStateDelegate<'TCachePrimaryKey,'TEntity>) dataSetRun = 
        match dataSetRun.RunIdPrev with
        | None          -> Seq.empty
        | Some runId    -> getByRunIdExcludeDeltaState.Invoke (SubscriptionDataSetId.value dataSetRun.SubscriptionDataSetId, RunId.value runId, DeltaStateType.DEL.ToString())
        |> Array.ofSeq

    // ------------------
    let execFindLatestCacheEntry cacheEntryOperation subscriptionDataSetId entityIdentifier = 
        match cacheEntryOperation.FindLatest.Invoke (SubscriptionDataSetId.value subscriptionDataSetId, entityIdentifier) with
        | NotFoundCacheEntry -> None
        | FoundCacheEntry cacheEntry -> Some cacheEntry
    let execInsertCache cacheEntryOperation cacheEntryProcessed =
        do cacheEntryOperation.Insert.Invoke (Processed.value cacheEntryProcessed)
        Persisted.ofProcessed cacheEntryProcessed
    let execUpdateCache cacheEntryOperation cacheEntryProcessed =
        do cacheEntryOperation.Update.Invoke (Processed.value cacheEntryProcessed)
        Persisted.ofProcessed cacheEntryProcessed

[<AutoOpen>]
module internal DeltaSnapshotCore = 
    let processDataSetEntity (subscriptionDataSetId, runId) (isEqual) (entity, cacheEntryFoundOption) =
        let printId = $"processDataSetEntity    {(entity :> IDataSetEntity).Identifier} {RunId.value runId}"
        match cacheEntryFoundOption with
        | None ->                                                           // fresh add
            printfnDebug     $"{printId} NotFound               ->  ADD insert"
            { CacheRowProcessed = Processed.create (DeltaSnapshotCacheRow.createAdd runId subscriptionDataSetId entity); CacheActionPending = Insert }
        | Some cacheEntryFound -> 
            match isEqual (cacheEntryFound.EntityDataCurrent, entity), DeltaState.fromStr cacheEntryFound.EntityDeltaCode with
            | true, Some DEL | false, Some DEL ->                            // prior delete has been added back
                printfnDebug $"{printId} Found    (DEL)         ->reADD insert"
                { CacheRowProcessed = Processed.create (DeltaSnapshotCacheRow.toAdd cacheEntryFound entity runId); CacheActionPending = Insert }
            | false, Some UPD | false, Some ADD ->                          // update
                printfnDebug $"{printId} Found <> (UPD/ADD)     ->  UPD insert"
                { CacheRowProcessed = Processed.create (DeltaSnapshotCacheRow.toUpd cacheEntryFound entity runId); CacheActionPending = Insert }
            | false, Some CUR | false, None (* treat invalid as CUR *) ->   // update
                printfnDebug $"{printId} Found <> (CUR)         ->  UPD update (from existing CUR or invalid)"
                { CacheRowProcessed = Processed.create (DeltaSnapshotCacheRow.toUpd cacheEntryFound entity runId); CacheActionPending = Update }
            | true, Some UPD | true, Some ADD  ->                           // no change, current
                printfnDebug $"{printId} Found == (UPD/ADD)     ->  CUR insert"                
                { CacheRowProcessed = Processed.create (DeltaSnapshotCacheRow.toCur cacheEntryFound runId); CacheActionPending = Insert }
            | true, Some CUR | true, None (* treat invalid as CUR *)    ->  // no change, curent
                printfnDebug $"{printId} Found == (CUR)         ->  CUR update (from existing CUR or invalid)"                
                { CacheRowProcessed = Processed.create (DeltaSnapshotCacheRow.toCur cacheEntryFound runId); CacheActionPending = Update }

    let persistProcessedDataSetEntity (insertUpdate : InsertUpdateCacheType<'TCachePrimaryKey, 'TEntity>) (processDataSetEntityTypeResult) : PersistedType<DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>> = 
        processDataSetEntityTypeResult.CacheRowProcessed
        |> match processDataSetEntityTypeResult.CacheActionPending with 
            | Insert -> 
            //printfnDebug $"persistProcessedDataSetEntity insert"                
                insertUpdate.Insert
            | Update -> 
            //printfnDebug $"persistProcessedDataSetEntity update"                
                insertUpdate.Update

    let processDataSet (dataSetRun: DataSetRunType, dataSet, isEqual, persistProcessed, findLatestCache, isFull) (transactionStartedState: TransactionStartedState) =
        transactionStartedState |> ignore
        let processEntityForDataSetRun = processDataSetEntity (dataSetRun.SubscriptionDataSetId, dataSetRun.RunIdCurr) isEqual
        let nonDeleteDeltaSnapshotsGross = 
            dataSet //|> Array.ofSeq
            |> Seq.map (fun entity -> processEntityForDataSetRun (entity, findLatestCache entity.Identifier))
            |> Array.ofSeq
            |> Array.Parallel.map persistProcessed
            |> Array.Parallel.map (fun cacheRowPersisted -> (cacheRowPersisted |> Persisted.value).EntityIdentifier, DeltaSnapshotMessage.ofDeltaSnapshotCacheRowPersisted isFull cacheRowPersisted)
        {   DataSetCount = DataSetCount.create nonDeleteDeltaSnapshotsGross.Length;
            DataSetEntityIds = nonDeleteDeltaSnapshotsGross |> Array.map (fun (entityId, _) -> entityId);
            DeltaSnapshotMessages = nonDeleteDeltaSnapshotsGross |> Seq.choose (fun (_, entityOpt) -> entityOpt) } // filter out None (occurs for isFull only) and descontruct Some

    let processNonDeleteCacheEntryAsDelete runIdCurr cacheEntryNonDelete =
        let printId = $"processCacheEntryDelete {cacheEntryNonDelete.EntityIdentifier} {RunId.value runIdCurr}"
        let cacheRowDelete = Processed.create (DeltaSnapshotCacheRow.toDel cacheEntryNonDelete runIdCurr)
        match DeltaState.fromStr cacheEntryNonDelete.EntityDeltaCode with 
        | Some DeltaStateType.UPD | Some DeltaStateType.ADD ->
            printfnDebug $"{printId} (UPD/ADD)              ->  DEL insert"
            Some { CacheRowProcessed = cacheRowDelete; CacheActionPending = Insert }
        | Some DeltaStateType.CUR (* CUR converts to DEL *) | None (* treat invalid as CUR *) ->
            printfnDebug $"{printId} (CUR)                  ->  DEL update"
            Some { CacheRowProcessed = cacheRowDelete; CacheActionPending = Update }
        | Some DeltaStateType.DEL (* DEL shoud not be possible, ignore *)  -> 
            printfnDebug $"{printId} (DEL)                  ->  ignore"
            None

    let processCacheResidualForDeletes dataSetEntityIds dataSetRun persistProcessed isFull (nonDeletesPrevious : DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>[]) =
        printfnDebug "cacheNonDeletes count: %i" nonDeletesPrevious.Length
        nonDeletesPrevious
            |> Array.filter (fun cacheEntryNonDelete -> dataSetEntityIds |> (Array.exists (fun id -> id = cacheEntryNonDelete.EntityIdentifier)) |> not) 
            |> Array.Parallel.map (processNonDeleteCacheEntryAsDelete dataSetRun.RunIdCurr)
            |> Array.choose id
            |> Array.Parallel.map persistProcessed
            |> Array.Parallel.map (DeltaSnapshotMessage.ofDeltaSnapshotCacheRowPersisted isFull)
            |> Array.choose id

    let private buildSnapshots<'TCachePrimaryKey, 'TEntity when 'TCachePrimaryKey :> Object and 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
        (isFull: bool) (subscription: ISubscription) (runId: RunIdType) (pullPublisherDataSetDelegate: PullPublisherDataSetDelegate<'TEntity>) 
        (emptyDataSetGetDeltasStrategy: EmptyDataSetGetDeltasStrategyType) (isEqualByValue: IsEqualByValueDelegate<'TEntity>) (cacheEntryOperation: CacheEntryOperation<'TCachePrimaryKey, 'TEntity>) =        

        printfnDebug "buildSnapshots %s %s" (if isFull then @"full" else @"deltas") (DateTimeOffset.Now.ToString())

        // initializations
        let subscriptionDataSetId = SubscriptionDataSetId.create subscription.SubscriptionDataSetId

        // prepare IO and misc functions
        let beginTransaction = IO.execBeginTransaction cacheEntryOperation.BeginTransaction
        let commitAndReturn = IO.execCommitAndReturn cacheEntryOperation.CommitTransaction 
        let rollbackAndReturn = IO.execRollbackAndReturn cacheEntryOperation.RollbackTransaction 
        let getLatestRunId = IO.execGetLatestRunIdCache cacheEntryOperation.GetRunIdLatestOfDataSet
        let getNonDeletesPreviousAsArray = IO.execGetPreviousNonDeletesCacheAsArray cacheEntryOperation.GetDataSetRunExcludeDeltaState
        let pullDataSet = fun () -> IO.execPullDataSet (pullPublisherDataSetDelegate, subscription)
        let findLatestCache = IO.execFindLatestCacheEntry cacheEntryOperation subscriptionDataSetId 
        let persistProcessed = persistProcessedDataSetEntity { Insert = IO.execInsertCache cacheEntryOperation; Update = IO.execUpdateCache cacheEntryOperation }
        let isEqual = fun (entity1, entity2) -> isEqualByValue.Invoke (entity1, entity2)

        try
            let dataSetRun = { SubscriptionDataSetId = subscriptionDataSetId; RunIdCurr = runId; RunIdPrev = getLatestRunId subscriptionDataSetId }
            let processDataSetResult = 
                dataSetRun
                |> beginTransaction
                |> processDataSet (dataSetRun, pullDataSet (), isEqual, persistProcessed, findLatestCache, isFull)

            match (not isFull && processDataSetResult.DataSetCount = DataSetCount.zero, emptyDataSetGetDeltasStrategy) with
                | false, EmptyDataSetGetDeltasStrategyType.RunFailure | false, EmptyDataSetGetDeltasStrategyType.RunSuccessWithBypass
                | false, EmptyDataSetGetDeltasStrategyType.DefaultProcessing | true, EmptyDataSetGetDeltasStrategyType.DefaultProcessing ->
                    let nonDeletesPrevious = getNonDeletesPreviousAsArray dataSetRun
                    let deltaSnapshots = 
                        processCacheResidualForDeletes processDataSetResult.DataSetEntityIds dataSetRun persistProcessed isFull nonDeletesPrevious
                        |> Seq.append <| processDataSetResult.DeltaSnapshotMessages
                        |> Array.ofSeq;      
                    commitAndReturn (RunResult.createSuccess (runId, @"Success.", deltaSnapshots, processDataSetResult.DataSetCount, DeltaCount.create deltaSnapshots.Length))
                | true, EmptyDataSetGetDeltasStrategyType.RunFailure -> 
                    rollbackAndReturn (RunResult.createFailure (runId, @"Publisher data set is empty."))
                | true, EmptyDataSetGetDeltasStrategyType.RunSuccessWithBypass ->
                    rollbackAndReturn (RunResult.createSuccess (runId, @"Run bypassed due to empty publisher data set.", Seq.empty, DataSetCount.zero, DeltaCount.zero))
        with
            | _ as ex -> 
                printfnDebug "Unexpected buildSnapshots exception %s %i %s %s" (DateTimeOffset.Now.ToString()) (RunId.value runId) (if isFull then @"full" else @"deltas") ex.Message
                rollbackAndReturn (RunResult.createFailure (runId, ex.Message))

    let getDeltas subscription = buildSnapshots false subscription
    let getDeltasAndCurrents subscription = buildSnapshots true subscription 