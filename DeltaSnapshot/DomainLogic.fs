//------------------------------------------------------------------------------
//    DeltaTracker.CLR
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
//open DeltaSnapshot

//[<AutoOpen>]
//module RunBeginTransaction = 
//    let bind f x = match x with | Success runId -> Ok runId | Failure str -> Error str

module internal RunId =
    let create = RunIdType 
    let value runId = let (RunIdType runIdValue) = runId in runIdValue
module internal DataSetId =
    let create = DataSetIdType
    let value dataSetId = let (DataSetIdType dataSetIdValue) = dataSetId in dataSetIdValue
module internal DeltaState =
    let private unionCache = Union.getUnionCases<DeltaStateType> |> Seq.cache 
    let fromStr delaStateCandidateStr = Union.createUnionCase<DeltaStateType> unionCache delaStateCandidateStr
module internal DeltaSnapshotMessage = // message
    let ofCacheEntry isFull (cacheEntry: ICacheEntryType<'TEntity>) = 
        match (isFull, DeltaState.fromStr cacheEntry.EntityDeltaCode) with
        | (false, Some CUR) | (false, None) | (true, None) -> 
            None    // do not return snapshot CUR if not null
        | (false, Some ADD) | (false,  Some DEL) | (false, Some UPD)
        | (true, Some ADD) | (true,  Some DEL) | (true, Some UPD) | (true, Some CUR) ->
            { Id = cacheEntry.EntityIdentifier; Delta = cacheEntry.EntityDeltaCode |> DeltaState.fromStr |> Option.defaultValue CUR;
                IsFull = isFull; Date = DateTimeOffset.Now; Cur = cacheEntry.EntityDataCurrent; Prv = cacheEntry.EntityDataPrevious } |> Some
            //new DeltaSnapshotType<'TEntity>(cacheEntry.EntityIdentifier, 
            //    cacheEntry.EntityDeltaCode |> DeltaState.fromStr |> Option.defaultValue CUR, 
            //    isFull, cacheEntry.EntityDataCurrent, cacheEntry.EntityDataPrevious) |> Some
            
module internal CacheEntry =
    type CurrentPreviousType<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> = { Current: 'TEntity; Previous: 'TEntity }
    let private create (DataSetIdType dataSetIdValue, RunIdType runIdValue, entityIdentifier, deltaState, rowCurrent, rowPrevious) = 
        new CacheEntryType<'TEntity>(dataSetIdValue, runIdValue, entityIdentifier, deltaState, DateTimeOffset.Now, rowCurrent, rowPrevious)
    let createAdd<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (dataSetId, runId, entity: 'TEntity) = 
        create (dataSetId, runId, entity.Identifier, ADD, entity, null) :> ICacheEntryType<'TEntity>
    let createUpd (dataSetId, runId, {Current=entCur; Previous=entPrv}) = 
        create (dataSetId, runId, entCur.Identifier, UPD, entCur, entPrv) :> ICacheEntryType<'TEntity>
    let createDel<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (dataSetId, runId, entity: 'TEntity) = 
        create (dataSetId, runId, entity.Identifier, DEL, null, entity) :> ICacheEntryType<'TEntity>
    let createCur<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (dataSetId, runId, entity: 'TEntity) = 
        create (dataSetId, runId, entity.Identifier, CUR, entity, null) :> ICacheEntryType<'TEntity>
    let converToDel<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (runId, cacheEntry: ICacheEntryType<'TEntity>) = 
        let cacheEntryNew = createDel<'TEntity> (DataSetId.create cacheEntry.DataSetId, runId, cacheEntry.EntityDataCurrent)
        cacheEntryNew.CacheEntryId <- cacheEntry.CacheEntryId // preserve PK in order to update
        cacheEntryNew 
    let migrateRun<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> (runId, cacheEntry: ICacheEntryType<'TEntity>) = 
        cacheEntry.RunId <- RunId.value runId
        cacheEntry.EntityDeltaDate <- DateTimeOffset.Now
        cacheEntry

[<AutoOpen>]
module internal DeltaTrackerIO = 
    let execPullSourceData (pull: PullSourceDataDelegate<'TEntity>, dataSetId) = pull.Invoke(DataSetId.value dataSetId)

    let execBeginTransaction (beginTransactionDelegte:BeginTransactionDelegate) = beginTransactionDelegte.Invoke

    let execCommitAndReturn (commitTransactionDelegate: CommitTransactionDelegate) (result:DeltaRunResultType<'TEntity>) =
        commitTransactionDelegate.Invoke |> ignore
        result

    let execRollbackAndReturn (rollbackTransactionDelegate: RollbackTransactionDelegate) (result:DeltaRunResultType<'TEntity>) =
        rollbackTransactionDelegate.Invoke |> ignore
        result

    let execGetLatestRunIdCache (findCacheLatestRunId: FindCacheLatestRunIdDelegate, dataSetId) =
        match findCacheLatestRunId.Invoke(DataSetId.value dataSetId) with
        | NotFoundRunId -> printfn "runIdPrev not found"; None
        | FoundRunId runId -> printfn "runIdPrev %i" (RunId.value runId); Some runId

    let execGetPreviousNonDeletesCacheAsList (getByRunIdExcludeDeltaState: GetCacheEntryByRunIdExcludeDeltaStateDelegate<'TEntity>, dataSetRun) = 
        match dataSetRun.RunIdPrev with
        | None          -> Seq.empty
        | Some runId    -> getByRunIdExcludeDeltaState.Invoke (DataSetId.value dataSetRun.DataSetId, RunId.value runId, DeltaStateType.DEL.ToString())
        |> List.ofSeq

    // ------------------
    let findLatestCacheEntry cacheEntryOperation dataSetId entityIdentifier = 
        match cacheEntryOperation.FindLatest.Invoke (DataSetId.value dataSetId, entityIdentifier) with
        | NotFoundCacheEntry -> None
        | FoundCacheEntry cacheEntry -> Some cacheEntry

    let execInsertCache cacheEntryOperation cacheEntry =
        cacheEntryOperation.Insert.Invoke(cacheEntry)

    let execUpdateCache cacheEntryOperation cacheEntry =
        cacheEntryOperation.Update.Invoke(cacheEntry)

[<AutoOpen>]
module internal DeltaTrackerCore = 
    let private processEntityAddDeltaInsert dataSetId entity runId cacheEntryOperation = 
        let cacheEntry = CacheEntry.createAdd (dataSetId, runId, entity)
        execInsertCache cacheEntryOperation cacheEntry
        cacheEntry

    // 1. tryInitRun 2. getSourceData 3. determine DSS list with cache insert, update, find (parallel?) 4. return DSS list (Addendum: track counts)
    let processEntity (dataSetId, runId, entity: 'TEntity, isEqual: IsEqualDelegate<'TEntity>, cacheEntryOperation, isFull) =
        let printId = $"processEntity {entity.Identifier} {RunId.value runId}"
        match findLatestCacheEntry cacheEntryOperation dataSetId entity.Identifier with
        | None ->       // fresh add
            printfn     $"{printId} NotFound             ->   ADD insert"
            (entity.Identifier, 
                processEntityAddDeltaInsert dataSetId entity runId cacheEntryOperation 
                |> DeltaSnapshotMessage.ofCacheEntry isFull)

        | Some cacheEntryFound -> 
            match isEqual.Invoke (cacheEntryFound.EntityDataCurrent, entity), DeltaState.fromStr cacheEntryFound.EntityDeltaCode with
            | false, None // treat invalid delta code as non-DEL
            | false, Some CUR | false, Some UPD | false, Some ADD -> // update
                printfn $"{printId} Found <>             ->   UPD insert"
                let cacheEntryNew = CacheEntry.createUpd (dataSetId, runId, {Current = entity; Previous = cacheEntryFound.EntityDataCurrent})
                execInsertCache cacheEntryOperation cacheEntryNew
                (entity.Identifier, 
                    cacheEntryNew 
                    |> DeltaSnapshotMessage.ofCacheEntry isFull)

            | true, Some DEL | false, Some DEL->  // prior delete has been added back
                printfn $"{printId} Found DEL            -> reADD insert"
                (entity.Identifier, 
                    processEntityAddDeltaInsert dataSetId entity runId cacheEntryOperation 
                    |> DeltaSnapshotMessage.ofCacheEntry isFull)

            | true, None // treat invalid delta code as non-DEL
            | true, Some UPD | true, Some ADD -> // no change, previous ADD or UPD
                printfn $"{printId} Found == (UPD/ADD)   ->   CUR insert"                
                let cacheEntryNew = CacheEntry.createCur (dataSetId, runId, entity) // insert CUR into cache
                execInsertCache cacheEntryOperation cacheEntryNew
                (entity.Identifier, 
                    cacheEntryNew 
                    |> DeltaSnapshotMessage.ofCacheEntry isFull) //if isFull then Some dss else None

            | true, Some CUR -> // no change, previous CUR
                printfn $"{printId} Found == (CUR)       ->   CUR update"
                let cacheEntryProgressed = CacheEntry.migrateRun (runId, cacheEntryFound)
                execUpdateCache cacheEntryOperation cacheEntryProgressed
                (entity.Identifier, 
                    cacheEntryProgressed
                    |> DeltaSnapshotMessage.ofCacheEntry isFull) //if isFull then Some dss else None

    let private processCacheEntryDelete (dataSetRun, (cacheEntryNonDelete: ICacheEntryType<'TEntity>), cacheEntryOperation, isFull) =
        match DeltaState.fromStr cacheEntryNonDelete.EntityDeltaCode with 
        | None                          // just treat invalid delta as CUR
        | Some DeltaStateType.CUR       // convert existing CUR to DEL
        | Some DeltaStateType.DEL ->    // DEL should not be possible but treat like CUR
            printfn $"processCacheEntryDelete {cacheEntryNonDelete.EntityDataCurrent.Identifier} {RunId.value dataSetRun.RunIdCurr}            ->   DEL update"
            let cacheEntryDelete = CacheEntry.converToDel (dataSetRun.RunIdCurr, cacheEntryNonDelete)
            execUpdateCache cacheEntryOperation cacheEntryDelete
            cacheEntryDelete |> DeltaSnapshotMessage.ofCacheEntry isFull
        | Some DeltaStateType.UPD | Some DeltaStateType.ADD ->
            printfn $"processCacheEntryDelete {cacheEntryNonDelete.EntityDataCurrent.Identifier} {RunId.value dataSetRun.RunIdCurr}            ->   DEL insert"
            let cacheEntryDelete = CacheEntry.createDel (dataSetRun.DataSetId, dataSetRun.RunIdCurr, cacheEntryNonDelete.EntityDataCurrent)
            execInsertCache cacheEntryOperation cacheEntryDelete
            cacheEntryDelete |> DeltaSnapshotMessage.ofCacheEntry isFull

    //let private buildSnapshotsCore<'TEntity when 'TEntity :> IEntityTrackable and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null>
    //    dataSetRun (nonDeleteDeltaSnapshots: DeltaSnapshotType<'TEntity> list) cacheEntryOperation isFull =
    //    printfn "nonDeleteDeltaSnapshots count: %i" nonDeleteDeltaSnapshots.Length
    //    let cacheNonDeletes = execGetPreviousNonDeletesCacheAsList (cacheEntryOperation.GetByRunIdExcludeDeltaState, dataSetRun)

    //    printfn "cacheNonDeletes count: %i" cacheNonDeletes.Length
    //    {   IsSuccess = true; 
    //        RunId = RunId.value dataSetRun.RunIdCurr; 
    //        DeltaSnapshots = 
    //            cacheNonDeletes
    //            |> Seq.filter (fun cacheEntryNonDelete -> nonDeleteDeltaSnapshots |> (Seq.exists (fun x -> x.Id = cacheEntryNonDelete.EntityIdentifier)) |> not) 
    //            |> Seq.map (fun cacheEntryNonDelete -> processCacheEntryDelete(dataSetRun, cacheEntryNonDelete, cacheEntryOperation, isFull))
    //            |> Seq.append <| nonDeleteDeltaSnapshots
    //            |> List.ofSeq; 
    //        ErrorMsgs = Seq.empty; 
    //        DataSetCount = 0; 
    //        DeltaCount = 0 }

    let private processNonDeletes (dataSetRun: DataSetRunType) pullSourceData (isEqual:IsEqualDelegate<'TEntity>) cacheEntryOperation isFull =
        pullSourceData()
        // TO DO: if no rows in data set, then bypass remaining steps
        |> Seq.map (fun entity -> processEntity(dataSetRun.DataSetId, dataSetRun.RunIdCurr, entity, isEqual, cacheEntryOperation, isFull) )
        //|> Seq.filter (fun (_, entityOpt) -> Option.isSome entityOpt)    // filters out Nones (occurs for isFull only)
        //|> Seq.map (fun (id, entityOpt) -> (id, entityOpt |> Option.get))       // deconstruct Some

    //let private buildSnapshotsOld<'TEntity when 'TEntity :> IEntityTrackable and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
    //        (isFull:bool) (dataSetId: DataSetIdType) (runId: RunIdType) 
    //        (pullSourceDataDelegate: PullSourceDataDelegate<'TEntity>) (isEqual:IsEqualDelegate<'TEntity>) (cacheEntryOperation: CacheEntryOperation<'TEntity>) = 
    //    printfn "buildSnapshots %s" (if isFull then @"full" else @"deltas")

    //    // prepare IO calls
    //    let beginTransaction = execBeginTransaction cacheEntryOperation.BeginTransaction
    //    let commitAndReturn = execCommitAndReturn cacheEntryOperation.CommitTransaction
    //    let rollbackAndReturn = execRollbackAndReturn cacheEntryOperation.RollbackTransaction
    //    let pullSourceData = fun() -> execPullSourceData (pullSourceDataDelegate, dataSetId)
    //    let getRunIdPrevious = fun() -> execGetLatestRunIdCache (cacheEntryOperation.GetRunIdLatest, dataSetId)
    //    let insertCache = fun() -> execInsertCache cacheEntryOperation
    //    let updateCache = fun() -> execInsertCache cacheEntryOperation

    //    try
    //        beginTransaction |> ignore
    //        // initializations
    //        let dataSetRun = { DataSetId = dataSetId; RunIdCurr = runId; RunIdPrev = getRunIdPrevious () }
    //        printfn "runIdCurr: %i" (RunId.value dataSetRun.RunIdCurr) 
    //        let nonDeleteSnapshots = (processNonDeletes dataSetRun pullSourceData isEqual cacheEntryOperation isFull) |> List.ofSeq
    //        printfn "AFTER processNonDeletes"
    //        buildSnapshotsCore dataSetRun nonDeleteSnapshots cacheEntryOperation isFull
    //        |> commitAndReturn 
    //    with
    //        | _ as ex -> 
    //            { IsSuccess = false; RunId = RunId.value runId; DeltaSnapshots = Seq.empty; ErrorMsgs = [ex.Message]; DataSetCount = 0; DeltaCount = 0 }
    //            |> rollbackAndReturn

    let private buildSnapshots<'TEntity when 'TEntity :> IDataSetEntity and 'TEntity : (new : unit -> 'TEntity) and 'TEntity : null> 
            (isFull:bool) (dataSetId: DataSetIdType) (runId: RunIdType) 
            (pullSourceDataDelegate: PullSourceDataDelegate<'TEntity>) (isEqual:IsEqualDelegate<'TEntity>) (cacheEntryOperation: CacheEntryOperation<'TEntity>) = 
        printfn "buildSnapshots %s" (if isFull then @"full" else @"deltas")

        // prepare IO calls
        let beginTransaction = execBeginTransaction cacheEntryOperation.BeginTransaction
        let commitAndReturn = execCommitAndReturn cacheEntryOperation.CommitTransaction
        let rollbackAndReturn = execRollbackAndReturn cacheEntryOperation.RollbackTransaction
        let pullSourceData = fun() -> execPullSourceData (pullSourceDataDelegate, dataSetId)
        let getRunIdPrevious = fun() -> execGetLatestRunIdCache (cacheEntryOperation.GetRunIdLatest, dataSetId)
        let insertCache = fun() -> execInsertCache cacheEntryOperation
        let updateCache = fun() -> execInsertCache cacheEntryOperation

        try
            // initializations
            let dataSetRun = { DataSetId = dataSetId; RunIdCurr = runId; RunIdPrev = getRunIdPrevious () }
            printfn "runIdCurr: %i" (RunId.value dataSetRun.RunIdCurr) 

            // TO DO: optionally delete all CURs in previous run async

            beginTransaction |> ignore
            let nonDeleteDeltaSnapshotsGross = (processNonDeletes dataSetRun pullSourceData isEqual cacheEntryOperation isFull) |> List.ofSeq
            let nonDeleteDeltaSnapshotIds = nonDeleteDeltaSnapshotsGross |> Seq.map (fun (id, _) -> id)
            let nonDeleteDeltaSnapshots = nonDeleteDeltaSnapshotsGross 
                                            |> Seq.choose (fun (_, entityOpt) -> entityOpt) // filter out None (occurs for isFull only) and descontruct Some
            printfn "AFTER processNonDeletes"

            let cacheNonDeletes = execGetPreviousNonDeletesCacheAsList (cacheEntryOperation.GetByRunIdExcludeDeltaState, dataSetRun)
            printfn "cacheNonDeletes count: %i" cacheNonDeletes.Length
            {   IsSuccess = true; 
                RunId = RunId.value dataSetRun.RunIdCurr; 
                DeltaSnapshots = 
                    cacheNonDeletes
                    |> Seq.filter (fun cacheEntryNonDelete -> nonDeleteDeltaSnapshotIds |> (Seq.exists (fun id -> id = cacheEntryNonDelete.EntityIdentifier)) |> not) 
                    |> Seq.map (fun cacheEntryNonDelete -> processCacheEntryDelete(dataSetRun, cacheEntryNonDelete, cacheEntryOperation, isFull))
                    |> Seq.choose id
                    |> Seq.append <| nonDeleteDeltaSnapshots
                        |> List.ofSeq; 
                ErrorMsgs = Seq.empty; 
                DataSetCount = 0; 
                DeltaCount = 0 }
            |> commitAndReturn 
        with
            | _ as ex -> 
                { IsSuccess = false; RunId = RunId.value runId; DeltaSnapshots = Seq.empty; ErrorMsgs = [ex.Message]; DataSetCount = 0; DeltaCount = 0 }
                |> rollbackAndReturn

    let getDeltas dataSetId = buildSnapshots false dataSetId
    let getDeltasAndCurrents dataSetId = buildSnapshots true dataSetId 