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
module internal Debug =
#if DEBUG    
    let printfnDebug = printfn  
    //let printfnDebug2 (format:Printf.TextWriterFormat<'a>) :'a = 'a
#else
    let printfnDebug = printfn  
#endif

module internal RunId =
    let create = RunIdType 
    let value runId = let (RunIdType runIdValue) = runId in runIdValue
module internal SubscriptionDataSetId =
    let create = SubscriptionDataSetIdType
    let value subscriptionDataSetId = let (SubscriptionDataSetIdType subscriptionDataSetIdValue) = subscriptionDataSetId in subscriptionDataSetIdValue
module internal DeltaState =
    let private unionCache = Union.getUnionCases<DeltaStateType> |> Seq.cache 
    let fromStr delaStateCandidateStr = Union.createUnionCase<DeltaStateType> unionCache delaStateCandidateStr
module internal DeltaSnapshotMessage = 
    let ofCacheEntry isFull (cacheEntry: ICacheEntryType<'TEntity>) = 
        match (isFull, DeltaState.fromStr cacheEntry.EntityDeltaCode) with
        | (false, Some CUR) | (false, None) | (true, None) -> 
            None    // do not return snapshot CUR if not full
        | (false, Some ADD) | (false,  Some DEL) | (false, Some UPD)
        | (true, Some ADD) | (true,  Some DEL) | (true, Some UPD) | (true, Some CUR) ->
            {   Id = cacheEntry.EntityIdentifier; Delta = cacheEntry.EntityDeltaCode |> DeltaState.fromStr |> Option.defaultValue CUR;
                IsFull = isFull; Date = cacheEntry.EntityDeltaDate; Cur = cacheEntry.EntityDataCurrent; Prv = cacheEntry.EntityDataPrevious } |> Some
            
module internal CacheEntry =
    type CurrentPreviousType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = { Current: 'TEntity; Previous: 'TEntity }
    let private create (SubscriptionDataSetIdType subscriptionDataSetIdValue, RunIdType runIdValue, entityIdentifier, deltaState, rowCurrent, rowPrevious) = 
        new CacheEntryType<'TEntity>(subscriptionDataSetIdValue, runIdValue, entityIdentifier, deltaState, DateTimeOffset.Now, rowCurrent, rowPrevious)
    let createAdd<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (subscriptionDataSetId, runId, entity: 'TEntity) = 
        create (subscriptionDataSetId, runId, entity.Identifier, ADD, entity, null) :> ICacheEntryType<'TEntity>
    let createUpd (subscriptionDataSetId, runId, {Current=entCur; Previous=entPrv}) = 
        create (subscriptionDataSetId, runId, entCur.Identifier, UPD, entCur, entPrv) :> ICacheEntryType<'TEntity>
    let createDel<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (subscriptionDataSetId, runId, entity: 'TEntity) = 
        //printfnDebug("CacheEntry.createDel")
        create (subscriptionDataSetId, runId, entity.Identifier, DEL, null, entity) :> ICacheEntryType<'TEntity>
    let createCur<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (subscriptionDataSetId, runId, entity: 'TEntity) = 
        create (subscriptionDataSetId, runId, entity.Identifier, CUR, entity, null) :> ICacheEntryType<'TEntity>

[<AutoOpen>]
module internal DeltaSnapshotIO = 
    let execPullDataSet (pullPublisherDataSet: PullPublisherDataSetDelegate<'TEntity>, subscription) = 
        pullPublisherDataSet.Invoke(subscription)
    let execBeginTransaction (beginTransactionDelegte: BeginTransactionDelegate) = 
        beginTransactionDelegte.Invoke
    let execCommitAndReturn (commitTransactionDelegate: CommitTransactionDelegate) (result: DeltaRunResultType<'TEntity>) =
        commitTransactionDelegate.Invoke |> ignore
        result
    let execRollbackAndReturn (rollbackTransactionDelegate: RollbackTransactionDelegate) (result: DeltaRunResultType<'TEntity>) =
        rollbackTransactionDelegate.Invoke |> ignore
        result
    let execGetLatestRunIdCache (findCacheLatestRunId: FindCacheEntryLatestRunIdDelegate, subscriptionDataSetId) =
        match findCacheLatestRunId.Invoke(SubscriptionDataSetId.value subscriptionDataSetId) with
        | NotFoundRunId -> printfnDebug "runIdPrev not found"; None
        | FoundRunId runId -> printfnDebug "runIdPrev %i" (RunId.value runId); Some runId
    let execGetPreviousNonDeletesCacheAsList (getByRunIdExcludeDeltaState: GetCacheEntryByRunIdExcludeDeltaStateDelegate<'TEntity>, dataSetRun) = 
        match dataSetRun.RunIdPrev with
        | None          -> Seq.empty
        | Some runId    -> getByRunIdExcludeDeltaState.Invoke (SubscriptionDataSetId.value dataSetRun.SubscriptionDataSetId, RunId.value runId, DeltaStateType.DEL.ToString())
        |> List.ofSeq

    // ------------------
    let execFindLatestCacheEntry cacheEntryOperation subscriptionDataSetId entityIdentifier = 
        match cacheEntryOperation.FindLatest.Invoke (SubscriptionDataSetId.value subscriptionDataSetId, entityIdentifier) with
        | NotFoundCacheEntry -> None
        | FoundCacheEntry cacheEntry -> Some cacheEntry
    let execInsertCache cacheEntryOperation cacheEntry =
        cacheEntryOperation.Insert.Invoke(cacheEntry)
    let asyncExecDeleteCurrentsPriorCache cacheEntryOperation subscriptionDataSetId runIdOption =
        async {
            match runIdOption with
            | None -> ()
            | Some runId -> cacheEntryOperation.DeleteDeltaStatePriorToRunId.Invoke(SubscriptionDataSetId.value subscriptionDataSetId, DeltaStateType.CUR.ToString(), RunId.value runId)
        }

[<AutoOpen>]
module internal DeltaSnapshotCore = 
    type DataSetProcessResultType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = 
        { DataSetCount: CountPrimitive; DataSetEntityIds: EntityIdentifierType seq; DeltaSnapshotMessages: DeltaSnapshotMessage<'TEntity> seq }

    let processDataSetEntity (subscriptionDataSetId, runId, entity: 'TEntity, isEqual: IsEqualDelegate<'TEntity>, insertCache, cacheEntryFoundOption: ICacheEntryType<'TEntity> option, isFull) =
        let printId = $"processDataSetEntity    {entity.Identifier} {RunId.value runId}"
        let cacheEntryNew = 
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
        insertCache cacheEntryNew
        (entity.Identifier, cacheEntryNew |> DeltaSnapshotMessage.ofCacheEntry isFull)

    let processDataSet (dataSetRun: DataSetRunType, pullDataSet, isEqual, insertCache, findLatestCache, isFull) =
        let nonDeleteDeltaSnapshotsGross = 
            pullDataSet () 
            |> Array.ofSeq
            |> Array.Parallel.map (fun entity -> processDataSetEntity(dataSetRun.SubscriptionDataSetId, dataSetRun.RunIdCurr, entity, isEqual, insertCache, (findLatestCache entity.Identifier), isFull) )
        {   DataSetCount = nonDeleteDeltaSnapshotsGross.Length;
            DataSetEntityIds = nonDeleteDeltaSnapshotsGross |> Seq.map (fun (id, _) -> id);
            DeltaSnapshotMessages = nonDeleteDeltaSnapshotsGross |> Seq.choose (fun (_, entityOpt) -> entityOpt) } // filter out None (occurs for isFull only) and descontruct Some

    let processCacheEntryDelete (dataSetRun, (cacheEntryNonDelete: ICacheEntryType<'TEntity>), insertCache, isFull) =
        let printId = $"processCacheEntryDelete {cacheEntryNonDelete.EntityDataCurrent.Identifier} {RunId.value dataSetRun.RunIdCurr}"
        match DeltaState.fromStr cacheEntryNonDelete.EntityDeltaCode with 
        | None (* treat invalid as CUR *) | Some DeltaStateType.CUR (* convert CUR to DEL *) | Some DeltaStateType.DEL (* DEL not possible, treat like CUR *) -> 
            printfnDebug $"{printId} (CUR)                   ->  DEL insert"
        | Some DeltaStateType.UPD | Some DeltaStateType.ADD ->
            printfnDebug $"{printId} (UPD/ADD)               ->  DEL insert"

        let cacheEntryDelete = CacheEntry.createDel (dataSetRun.SubscriptionDataSetId, dataSetRun.RunIdCurr, cacheEntryNonDelete.EntityDataCurrent)
        insertCache cacheEntryDelete
        cacheEntryDelete |> DeltaSnapshotMessage.ofCacheEntry isFull

    let private buildSnapshots<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (isFull: bool) (subscription: ISubscription) (runId: RunIdType) 
            (pullPublisherDataSetDelegate: PullPublisherDataSetDelegate<'TEntity>) (emptyDataSetGetDeltasStrategy: EmptyDataSetGetDeltasStrategyType) 
            (isEqual: IsEqualDelegate<'TEntity>) (cacheEntryOperation: CacheEntryOperation<'TEntity>) = 
        printfnDebug "buildSnapshots %s" (if isFull then @"full" else @"deltas")

        let subscriptionDataSetId = SubscriptionDataSetId.create subscription.SubscriptionDataSetId

        // prepare IO calls
        let beginTransaction = execBeginTransaction cacheEntryOperation.BeginTransaction
        let commitAndReturn = execCommitAndReturn cacheEntryOperation.CommitTransaction
        let rollbackAndReturn = execRollbackAndReturn cacheEntryOperation.RollbackTransaction
        let pullDataSet = fun() -> execPullDataSet (pullPublisherDataSetDelegate, subscription)
        let getRunIdPrevious = fun() -> execGetLatestRunIdCache (cacheEntryOperation.GetRunIdLatest, subscriptionDataSetId)
        let findLatestCache = fun entityIdentifier -> execFindLatestCacheEntry cacheEntryOperation subscriptionDataSetId entityIdentifier
        let insertCache = fun cacheEntry -> execInsertCache cacheEntryOperation cacheEntry
        let asyncDeleteCurrentsPriorTo = fun runIdOption -> asyncExecDeleteCurrentsPriorCache cacheEntryOperation subscriptionDataSetId runIdOption

        try
            // initializations
            let dataSetRun = { SubscriptionDataSetId = subscriptionDataSetId; RunIdCurr = runId; RunIdPrev = getRunIdPrevious () }

            beginTransaction |> ignore
            Async.RunSynchronously (asyncDeleteCurrentsPriorTo dataSetRun.RunIdPrev) // TO DO: make optional 
            let processDataSetResult = processDataSet (dataSetRun, pullDataSet, isEqual, insertCache, findLatestCache, isFull)
            printfnDebug "AFTER processDataSet"

            match (not isFull && processDataSetResult.DataSetCount = 0, emptyDataSetGetDeltasStrategy) with
                | true, EmptyDataSetGetDeltasStrategyType.RunFailure -> 
                    failwith "Data set is empty." 
                | true, EmptyDataSetGetDeltasStrategyType.RunSuccessWithBypass ->
                    {   IsSuccess = true; RunId = RunId.value runId; ErrorMsgs = ["Run bypassed due to empty data set."]; 
                        DeltaSnapshots = Seq.empty; DataSetCount = 0; DeltaCount = 0 }
                | false, EmptyDataSetGetDeltasStrategyType.RunFailure | false, EmptyDataSetGetDeltasStrategyType.RunSuccessWithBypass
                | false, EmptyDataSetGetDeltasStrategyType.DefaultProcessing | true, EmptyDataSetGetDeltasStrategyType.DefaultProcessing ->
                    let cacheNonDeletes = execGetPreviousNonDeletesCacheAsList (cacheEntryOperation.GetByRunIdExcludeDeltaState, dataSetRun)
                    printfnDebug "cacheNonDeletes count: %i" cacheNonDeletes.Length
                    let deltaSnapshots = 
                        cacheNonDeletes
                        |> Seq.filter (fun cacheEntryNonDelete -> processDataSetResult.DataSetEntityIds |> (Seq.exists (fun id -> id = cacheEntryNonDelete.EntityIdentifier)) |> not) 
                        |> Seq.map (fun cacheEntryNonDelete -> processCacheEntryDelete(dataSetRun, cacheEntryNonDelete, insertCache, isFull))
                        |> Seq.choose id
                        |> Seq.append <| processDataSetResult.DeltaSnapshotMessages
                        |> List.ofSeq; 
 
                    {   IsSuccess = true; RunId = RunId.value dataSetRun.RunIdCurr; ErrorMsgs = ["Success."];
                        DeltaSnapshots = deltaSnapshots; DataSetCount = processDataSetResult.DataSetCount; DeltaCount = deltaSnapshots.Length }
                    |> commitAndReturn
        with
            | _ as ex -> 
                {   IsSuccess = false; RunId = RunId.value runId; ErrorMsgs = [ex.Message]; 
                    DeltaSnapshots = Seq.empty; DataSetCount = 0; DeltaCount = 0 }
                |> rollbackAndReturn

    let getDeltas subscription = buildSnapshots false subscription
    let getDeltasAndCurrents subscription = buildSnapshots true subscription 