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
    let ofCacheEntry isFull (cacheEntry: ICacheEntryType<'TEntity>) = 
        match (isFull, DeltaState.fromStr cacheEntry.EntityDeltaCode) with
        | (false, Some CUR) | (false, None) | (true, None) -> 
            None    // do not return a CUR snapshot if is not a full
        | (false, Some ADD) | (false,  Some DEL) | (false, Some UPD)
        | (true, Some ADD) | (true,  Some DEL) | (true, Some UPD) | (true, Some CUR) ->
            {   Id = cacheEntry.EntityIdentifier; Delta = cacheEntry.EntityDeltaCode |> DeltaState.fromStr |> Option.defaultValue CUR;
                IsFull = isFull; Date = cacheEntry.EntityDeltaDate; Cur = cacheEntry.EntityDataCurrent; Prv = cacheEntry.EntityDataPrevious } |> Some
    let ofDeltaSnapshotCacheRow isFull (cacheRow: DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>) = 
        match (isFull, DeltaState.fromStr cacheRow.EntityDeltaCode) with
        | (false, Some CUR) | (false, None) | (true, None) -> 
            None    // do not return a CUR snapshot if is not a full
        | (false, Some ADD) | (false,  Some DEL) | (false, Some UPD)
        | (true, Some ADD) | (true,  Some DEL) | (true, Some UPD) | (true, Some CUR) ->
            {   Id = cacheRow.EntityIdentifier; Delta = cacheRow.EntityDeltaCode |> DeltaState.fromStr |> Option.defaultValue CUR;
                IsFull = isFull; Date = cacheRow.EntityDeltaDate; Cur = cacheRow.EntityDataCurrent; Prv = cacheRow.EntityDataPrevious } |> Some

module internal DeltaRunResult =
    let createSuccess (RunIdType runIdValue, errorMessage, deltaSnapshots, DataSetCountType dataSetCountValue, DeltaCountType deltaCountValue) = 
        { IsSuccess = true; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = deltaSnapshots; DataSetCount = dataSetCountValue; DeltaCount = deltaCountValue }
    let createFailure (RunIdType runIdValue, errorMessage) = 
        { IsSuccess = false; RunId = runIdValue; ErrorMsgs = [errorMessage]; DeltaSnapshots = Seq.empty; DataSetCount = 0; DeltaCount = 0 }

type CurrentPreviousType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = { Current: 'TEntity; Previous: 'TEntity }
module internal CacheEntry =
    let private create (SubscriptionDataSetIdType subscriptionDataSetIdValue, RunIdType runIdValue, entityIdentifier, deltaState, rowCurrent, rowPrevious) = 
        new CacheEntryType<'TEntity>(subscriptionDataSetIdValue, runIdValue, entityIdentifier, deltaState, DateTimeOffset.Now, rowCurrent, rowPrevious)
    let createAdd<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (subscriptionDataSetId, runId, entity: 'TEntity) = 
        create (subscriptionDataSetId, runId, entity.Identifier, ADD, entity, null) :> ICacheEntryType<'TEntity>
    let createUpd (subscriptionDataSetId, runId, {Current=entCur; Previous=entPrv}) = 
        create (subscriptionDataSetId, runId, entCur.Identifier, UPD, entCur, entPrv) :> ICacheEntryType<'TEntity>
    let createDel<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (subscriptionDataSetId, runId, entity: 'TEntity) = 
        create (subscriptionDataSetId, runId, entity.Identifier, DEL, null, entity) :> ICacheEntryType<'TEntity>
    let createCur<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (subscriptionDataSetId, runId, entity: 'TEntity) = 
        create (subscriptionDataSetId, runId, entity.Identifier, CUR, entity, null) :> ICacheEntryType<'TEntity>

module internal DeltaSnapshotCacheRow =
    let private progress row (RunIdType runIdValue) = { row with RunId = runIdValue; EntityDeltaDate = DateTimeOffset.Now }
    // fromDelToAdd-I
    let toAdd existingRow entityLatest = 
        { existingRow with EntityDeltaCode = DeltaStateType.ADD.ToString(); EntityDataPrevious = null; EntityDataCurrent = entityLatest }
        |> progress 
    // fromCurToUpd-U, fromAddToUpd-I
    let toUpd existingRow entityLatest = 
        { existingRow with EntityDeltaCode = DeltaStateType.UPD.ToString(); EntityDataPrevious = existingRow.EntityDataCurrent; EntityDataCurrent = entityLatest }
        |> progress 
    // fromCurToDel-U, fromAddToDel-I, fromUpdToDel-I
    let toDel existingRow = 
        { existingRow with EntityDeltaCode = DeltaStateType.DEL.ToString(); EntityDataPrevious = existingRow.EntityDataCurrent; EntityDataCurrent = null }
        |> progress 
    // fromAddToCur-I, fromUpdToCur-I
    let toCur existingRow = 
        { existingRow with EntityDeltaCode = DeltaStateType.CUR.ToString(); EntityDataPrevious = null }
        |> progress 
    // -I
    let createAdd (RunIdType runIdValue) (SubscriptionDataSetIdType subscriptionDataSetIdValue) entityLatest = 
        {   PrimaryKey = Unchecked.defaultof<'TCachePrimaryKey>; SubscriptionDataSetId = subscriptionDataSetIdValue; RunId = runIdValue; EntityDeltaDate = DateTimeOffset.Now;
            EntityDeltaCode = DeltaStateType.ADD.ToString();  EntityDataPrevious = null; EntityDataCurrent = entityLatest; EntityIdentifier = entityLatest.Identifier }

[<AutoOpen>]
module internal DeltaSnapshotIO = 
    let execPullDataSet (pullPublisherDataSet: PullPublisherDataSetDelegate<'TEntity>, subscription) = 
        pullPublisherDataSet.Invoke (subscription)
    let execBeginTransaction (beginTransactionDelegte: BeginTransactionDelegate) = 
        beginTransactionDelegte.Invoke
    let execCommitAndReturn (commitTransactionDelegate: CommitTransactionDelegate) (result: DeltaRunResultType<'TEntity>) =
        commitTransactionDelegate.Invoke |> ignore
        result
    let execRollbackAndReturn (rollbackTransactionDelegate: RollbackTransactionDelegate) (result: DeltaRunResultType<'TEntity>) =
        rollbackTransactionDelegate.Invoke |> ignore
        result
    let execGetLatestRunIdCache (findCacheLatestRunId: FindCacheEntryLatestRunIdDelegate, subscriptionDataSetId) =
        match findCacheLatestRunId.Invoke (SubscriptionDataSetId.value subscriptionDataSetId) with
        | NotFoundRunId -> printfnDebug "runIdPrev not found"; None
        | FoundRunId runId -> printfnDebug "runIdPrev %i" (RunId.value runId); Some runId
    let execGetPreviousNonDeletesCacheAsArray (getByRunIdExcludeDeltaState: GetCacheEntryByRunIdExcludeDeltaStateDelegate<'TEntity>, dataSetRun) = 
        match dataSetRun.RunIdPrev with
        | None          -> Seq.empty
        | Some runId    -> getByRunIdExcludeDeltaState.Invoke (SubscriptionDataSetId.value dataSetRun.SubscriptionDataSetId, RunId.value runId, DeltaStateType.DEL.ToString())
        |> Array.ofSeq

    // ------------------
    let execFindLatestCacheEntry cacheEntryOperation subscriptionDataSetId entityIdentifier = 
        match cacheEntryOperation.FindLatest.Invoke (SubscriptionDataSetId.value subscriptionDataSetId, entityIdentifier) with
        | NotFoundCacheEntry -> None
        | FoundCacheEntry cacheEntry -> Some cacheEntry
    let execInsertCache cacheEntryOperation cacheEntry =
        cacheEntryOperation.Insert.Invoke (cacheEntry)
    let asyncExecDeleteCurrentsPriorCache cacheEntryOperation subscriptionDataSetId runIdOption =
        async {
            match runIdOption with
            | None -> ()
            | Some runId -> cacheEntryOperation.DeleteDeltaStatePriorToRunId.Invoke (SubscriptionDataSetId.value subscriptionDataSetId, DeltaStateType.CUR.ToString(), RunId.value runId)
        }

[<AutoOpen>]
module internal DeltaSnapshotCore = 
    type DataSetProcessResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
        { DataSetCount: DataSetCountType; DataSetEntityIds: EntityIdentifierType seq; DeltaSnapshotMessages: DeltaSnapshotMessage<'TEntity> seq }

    // TO DO: return tuple of cache entry and cache action (insert/update)
    let processDataSetEntity (subscriptionDataSetId, runId, entity: 'TEntity, isEqual: IsEqualDelegate<'TEntity>, cacheEntryFoundOption: ICacheEntryType<'TEntity> option) =
        let printId = $"processDataSetEntity    {entity.Identifier} {RunId.value runId}"
        match cacheEntryFoundOption with
        | None ->       // fresh add
            printfnDebug     $"{printId} NotFound                ->  ADD insert"
            CacheEntry.createAdd (subscriptionDataSetId, runId, entity)                
        | Some cacheEntryFound -> 
            match isEqual.Invoke (cacheEntryFound.EntityDataCurrent, entity), DeltaState.fromStr cacheEntryFound.EntityDeltaCode with
            | false, None // treat invalid delta code as non-DEL
            | false, Some CUR | false, Some UPD | false, Some ADD -> // update
                printfnDebug $"{printId} Found <>                ->  UPD insert"
                CacheEntry.createUpd (subscriptionDataSetId, runId, {Current = entity; Previous = cacheEntryFound.EntityDataCurrent})                    
            | true, Some DEL | false, Some DEL->  // prior delete has been added back
                printfnDebug $"{printId} Found (DEL)             ->reADD insert"
                CacheEntry.createAdd (subscriptionDataSetId, runId, entity)                    
            | true, None // treat invalid delta code as non-DEL
            | true, Some UPD | true, Some ADD | true, Some CUR -> // no change, previous ADD, UPD or CUR
                printfnDebug $"{printId} Found == (UPD/ADD/CUR)  ->  CUR insert"                
                CacheEntry.createCur (subscriptionDataSetId, runId, entity) // insert CUR into cache

    let processDataSet<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>(dataSetRun: DataSetRunType, dataSet, isEqual, insertCache, findLatestCache, isFull) =
        let nonDeleteDeltaSnapshotsGross = 
            dataSet
            |> Array.ofSeq
            |> Array.Parallel.map (fun (entity: 'TEntity) -> processDataSetEntity (dataSetRun.SubscriptionDataSetId, dataSetRun.RunIdCurr, entity, isEqual, (findLatestCache entity.Identifier)) )
            |> Array.Parallel.map (fun (cacheEntry: ICacheEntryType<'TEntity>) -> cacheEntry |> insertCache |> ignore; cacheEntry)
            |> Array.Parallel.map (fun (cacheEntry: ICacheEntryType<'TEntity>) -> (cacheEntry.EntityIdentifier, DeltaSnapshotMessage.ofCacheEntry isFull cacheEntry) )
        {   DataSetCount = DataSetCount.create nonDeleteDeltaSnapshotsGross.Length;
            DataSetEntityIds = nonDeleteDeltaSnapshotsGross |> Seq.map (fun (id, _) -> id);
            DeltaSnapshotMessages = nonDeleteDeltaSnapshotsGross |> Seq.choose (fun (_, entityOpt) -> entityOpt) } // filter out None (occurs for isFull only) and descontruct Some

    let processNonDeleteCacheEntryAsDelete (dataSetRun, (cacheEntryNonDelete: ICacheEntryType<'TEntity>)) =
        let printId = $"processCacheEntryDelete {cacheEntryNonDelete.EntityDataCurrent.Identifier} {RunId.value dataSetRun.RunIdCurr}"
        match DeltaState.fromStr cacheEntryNonDelete.EntityDeltaCode with 
        | None (* treat invalid as CUR *) | Some DeltaStateType.CUR (* convert CUR to DEL *) | Some DeltaStateType.DEL (* DEL not possible, treat like CUR *) -> 
            printfnDebug $"{printId} (CUR)                   ->  DEL insert"
        | Some DeltaStateType.UPD | Some DeltaStateType.ADD ->
            printfnDebug $"{printId} (UPD/ADD)               ->  DEL insert"
        CacheEntry.createDel (dataSetRun.SubscriptionDataSetId, dataSetRun.RunIdCurr, cacheEntryNonDelete.EntityDataCurrent)

    let processCacheResidualForDeletes processDataSetResult dataSetRun insertCache isFull (nonDeletesPrevious : ICacheEntryType<'TEntity>[]) =
        printfnDebug "cacheNonDeletes count: %i" nonDeletesPrevious.Length
        nonDeletesPrevious
            |> Array.filter (fun cacheEntryNonDelete -> processDataSetResult.DataSetEntityIds |> (Seq.exists (fun id -> id = cacheEntryNonDelete.EntityIdentifier)) |> not) 
            |> Array.Parallel.map (fun cacheEntryNonDelete -> processNonDeleteCacheEntryAsDelete(dataSetRun, cacheEntryNonDelete))
            |> Array.Parallel.map (fun cacheEntryNonDelete -> cacheEntryNonDelete |> insertCache |> ignore; cacheEntryNonDelete)
            |> Array.Parallel.map (fun cacheEntryNonDelete -> DeltaSnapshotMessage.ofCacheEntry isFull cacheEntryNonDelete)
            |> Array.choose id
            |> Seq.append <| processDataSetResult.DeltaSnapshotMessages
            |> Array.ofSeq;      

    let private buildSnapshots 
            (isFull: bool) (subscription: ISubscription) (runId: RunIdType) 
            (pullPublisherDataSetDelegate: PullPublisherDataSetDelegate<'TEntity>) (emptyDataSetGetDeltasStrategy: EmptyDataSetGetDeltasStrategyType) 
            (isEqual: IsEqualDelegate<'TEntity>) (cacheEntryOperation: CacheEntryOperation<'TCachePrimaryKey, 'TEntity>) = 
        printfnDebug "buildSnapshots %s" (if isFull then @"full" else @"deltas")

        // prepare transaction management calls
        let beginTransaction = execBeginTransaction cacheEntryOperation.BeginTransaction
        let commitAndReturn = execCommitAndReturn cacheEntryOperation.CommitTransaction 
        let rollbackAndReturn = execRollbackAndReturn cacheEntryOperation.RollbackTransaction 

        try
            // initializations
            let subscriptionDataSetId = SubscriptionDataSetId.create subscription.SubscriptionDataSetId
            let dataSetRun = { SubscriptionDataSetId = subscriptionDataSetId; RunIdCurr = runId; RunIdPrev = execGetLatestRunIdCache (cacheEntryOperation.GetRunIdLatest, subscriptionDataSetId) }

            // prepare IO calls
            let pullDataSet = fun () -> execPullDataSet (pullPublisherDataSetDelegate, subscription)
            let findLatestCache = fun entityIdentifier -> execFindLatestCacheEntry cacheEntryOperation subscriptionDataSetId entityIdentifier
            let insertCache = fun cacheEntry -> execInsertCache cacheEntryOperation cacheEntry
            let asyncDeleteCurrentsPriorTo = fun runIdOption -> asyncExecDeleteCurrentsPriorCache cacheEntryOperation subscriptionDataSetId runIdOption
            let getNonDeletesPreviousAsArray = fun () -> execGetPreviousNonDeletesCacheAsArray (cacheEntryOperation.GetByRunIdExcludeDeltaState, dataSetRun)

            beginTransaction |> ignore
            Async.RunSynchronously (asyncDeleteCurrentsPriorTo dataSetRun.RunIdPrev) // TO DO: make optional 
            let dataSet = pullDataSet () 
            let processDataSetResult = processDataSet (dataSetRun, dataSet, isEqual, insertCache, findLatestCache, isFull)
            printfnDebug "AFTER processDataSet"
            match (not isFull && processDataSetResult.DataSetCount = DataSetCount.zero, emptyDataSetGetDeltasStrategy) with
                | true, EmptyDataSetGetDeltasStrategyType.RunFailure -> 
                    rollbackAndReturn (DeltaRunResult.createFailure (runId, @"Data set is empty."))
                | true, EmptyDataSetGetDeltasStrategyType.RunSuccessWithBypass ->
                    rollbackAndReturn (DeltaRunResult.createSuccess (runId, @"Run bypassed due to empty data set.", Seq.empty, DataSetCount.zero, DeltaCount.zero))
                | false, EmptyDataSetGetDeltasStrategyType.RunFailure | false, EmptyDataSetGetDeltasStrategyType.RunSuccessWithBypass
                | false, EmptyDataSetGetDeltasStrategyType.DefaultProcessing | true, EmptyDataSetGetDeltasStrategyType.DefaultProcessing ->
                    let nonDeletesPrevious = getNonDeletesPreviousAsArray ()
                    let deltaSnapshots = processCacheResidualForDeletes processDataSetResult dataSetRun insertCache isFull nonDeletesPrevious
                    commitAndReturn (DeltaRunResult.createSuccess (runId, @"Success.", deltaSnapshots, processDataSetResult.DataSetCount, DeltaCount.create deltaSnapshots.Length))
        with
            | _ as ex -> rollbackAndReturn (DeltaRunResult.createFailure (runId, ex.Message))

    let getDeltas subscription = buildSnapshots false subscription
    let getDeltasAndCurrents subscription = buildSnapshots true subscription 