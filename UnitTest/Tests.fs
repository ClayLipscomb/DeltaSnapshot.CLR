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

module Tests

#if DEBUG

open System
open Xunit
open DeltaSnapshot

[<AllowNullLiteral>]
type Entity (identifier, propertyString) =
    member this.Identifier = identifier
    interface IDataSetEntity with
        member this.Identifier = this.Identifier
    member this.PropertyString = propertyString
    new() = Entity (String.Empty, String.Empty)

[<AutoOpen>]
module internal TestFunc =
    let isEqualByValue (entity1: Entity) (entity2: Entity) = 
        if (entity1 = null && entity2 = null) then
            true
        else if (entity1 = null || entity2 = null) then
            false
        else 
            (entity1:>IDataSetEntity).Identifier = (entity2:>IDataSetEntity).Identifier && entity1.PropertyString = entity2.PropertyString 
    let isEqualDelegate = IsEqualByValueDelegate (isEqualByValue)

    // default data values
    let (runIdValuePrev, runIdValueCurr, subscriptionDataSetIdValue) = (1L, 2L, 14); 
    let deltaCodeInvalid = "XYZ"
    let (entityPrev, entityCurr, entityUpdate) = ( Entity ("1", "PREV"), Entity ("1", "CURR"), Entity ("1", "UPDATE") )

    // cache row func
    let isCacheRowCoreValid (cacheRow: DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>) = 
        cacheRow.RunId = runIdValueCurr && cacheRow.SubscriptionDataSetId = subscriptionDataSetIdValue && cacheRow.EntityIdentifier = (entityCurr:>IDataSetEntity).Identifier
    let deltaSnapshotCacheRowBaseAdd = 
        fun () -> ApiTest.deltaSnapshotCacheRowCreateAdd runIdValuePrev subscriptionDataSetIdValue entityCurr
    let deltaSnapshotCacheRowBaseDel = 
        fun () -> ApiTest.deltaSnapshotCacheRowToDel (deltaSnapshotCacheRowBaseAdd ()) runIdValueCurr
    let deltaSnapshotCacheRowBaseUpd = 
        fun () -> ApiTest.deltaSnapshotCacheRowToUpd (ApiTest.deltaSnapshotCacheRowCreateAdd runIdValuePrev subscriptionDataSetIdValue entityPrev) entityCurr runIdValueCurr
    let deltaSnapshotCacheRowBaseCur = 
        fun () -> ApiTest.deltaSnapshotCacheRowToCur (ApiTest.deltaSnapshotCacheRowCreateAdd runIdValuePrev subscriptionDataSetIdValue entityPrev) runIdValueCurr

    let testProcessDataSetEntityDataSetRun (entity, cacheRow) = 
        ApiTest.testProcessDataSetEntity<System.Int64, Entity> (runIdValueCurr, subscriptionDataSetIdValue) (isEqualDelegate) (entity, cacheRow)
    let testProcessNonDeleteCacheEntryAsDelete cacheRow =
        ApiTest.testProcessNonDeleteCacheEntryAsDelete runIdValueCurr cacheRow

    let isEqualMessageAndCacheRow isFull ((message: DeltaSnapshotMessage<Entity>), (cacheRow: DeltaSnapshotCacheRowType<Int64, Entity>)) =
        (   message.Id = cacheRow.EntityIdentifier && message.Delta.ToString() = cacheRow.EntityDeltaCode && message.IsFull = isFull && message.Date = cacheRow.EntityDeltaDate
            && (isEqualByValue message.Cur cacheRow.EntityDataCurrent) && (isEqualByValue message.Prv cacheRow.EntityDataPrevious) )

////////////////////
// deltaStateFromStr 1
[<Fact>]
let ``DeltaStateType`` () =
    Assert.Equal(DeltaStateType.CUR, @"CUR" |> ApiTest.deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.ADD, @"ADD" |> ApiTest.deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.UPD, @"UPD" |> ApiTest.deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.DEL, @"DEL" |> ApiTest.deltaStateFromStr |> Option.get)
    Assert.Equal(None, @"XYZ" |> ApiTest.deltaStateFromStr)

///////////////////////
// processDataSetEntity 11
[<Fact>]
let ``ProcessDataSetEntity-AddFromNotFound`` () =
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, None) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = ADD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-ReAddFromDelIsEqual`` () =
    let deltaSnapshotCacheRowBaseDel = deltaSnapshotCacheRowBaseDel () 
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, Some deltaSnapshotCacheRowBaseDel) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = ADD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-ReAddFromDelIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseDel = deltaSnapshotCacheRowBaseDel () 
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityUpdate, Some deltaSnapshotCacheRowBaseDel) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = ADD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityUpdate) && cacheRowNew.EntityDataPrevious = null
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromUpdIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseUpd = deltaSnapshotCacheRowBaseUpd () 
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityUpdate, Some deltaSnapshotCacheRowBaseUpd) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityUpdate) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromAddIsNotEqual`` () =
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityUpdate, Some (deltaSnapshotCacheRowBaseAdd ())) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityUpdate) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromCurIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseCur = ApiTest.deltaSnapshotCacheRowToCur (deltaSnapshotCacheRowBaseAdd ()) runIdValuePrev
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityUpdate, Some deltaSnapshotCacheRowBaseCur) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityUpdate) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
        && isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromInvalidIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseInvalid = { (deltaSnapshotCacheRowBaseAdd ()) with EntityDeltaCode = deltaCodeInvalid }
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityUpdate, Some deltaSnapshotCacheRowBaseInvalid) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityUpdate) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
        && isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-CurFromAddIsEqual`` () =
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, Some (deltaSnapshotCacheRowBaseAdd ())) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = CUR.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-CurFromUpdIsEqual`` () =
    let deltaSnapshotCacheRowBaseUpd = deltaSnapshotCacheRowBaseUpd ()
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, Some deltaSnapshotCacheRowBaseUpd) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = CUR.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-CurFromCurIsEqual`` () =
    let deltaSnapshotCacheRowBaseCur = ApiTest.deltaSnapshotCacheRowToCur (deltaSnapshotCacheRowBaseAdd ()) runIdValuePrev
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, Some deltaSnapshotCacheRowBaseCur) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = CUR.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-CurFromInvalidIsEqual`` () =
    let deltaSnapshotCacheRowBaseInvalid = { (deltaSnapshotCacheRowBaseAdd ()) with EntityDeltaCode = deltaCodeInvalid }
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, Some deltaSnapshotCacheRowBaseInvalid) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = CUR.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && isCacheActionUpdate
    )

/////////////////////////////////////
// processNonDeleteCacheEntryAsDelete 5
[<Fact>]
let ``ProcessNonDeleteCacheEntryAsDelete-FromAdd`` () =
    let deltaSnapshotCacheRowBaseAdd = deltaSnapshotCacheRowBaseAdd ()
    match testProcessNonDeleteCacheEntryAsDelete deltaSnapshotCacheRowBaseAdd with
    | Some (cacheRowNew, isCacheActionUpdate) ->
        Assert.True ( isCacheRowCoreValid cacheRowNew
            && cacheRowNew.EntityDeltaCode = DEL.ToString()
            && (cacheRowNew.EntityDataCurrent = null && isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
            && not isCacheActionUpdate 
        )
    | None -> Assert.True(false)

[<Fact>]
let ``ProcessNonDeleteCacheEntryAsDelete-FromUpd`` () =
    let deltaSnapshotCacheRowBaseUpd = deltaSnapshotCacheRowBaseUpd ()
    match testProcessNonDeleteCacheEntryAsDelete deltaSnapshotCacheRowBaseUpd with
    | Some (cacheRowNew, isCacheActionUpdate) ->
        Assert.True ( isCacheRowCoreValid cacheRowNew
            && cacheRowNew.EntityDeltaCode = DEL.ToString()
            && (cacheRowNew.EntityDataCurrent = null && isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
            && not isCacheActionUpdate 
        )
    | None -> Assert.True(false)

[<Fact>]
let ``ProcessNonDeleteCacheEntryAsDelete-FromCur`` () =
    let deltaSnapshotCacheRowBaseCur = ApiTest.deltaSnapshotCacheRowToCur (deltaSnapshotCacheRowBaseAdd ()) runIdValuePrev
    match testProcessNonDeleteCacheEntryAsDelete deltaSnapshotCacheRowBaseCur with
    | Some (cacheRowNew, isCacheActionUpdate) ->
        Assert.True ( isCacheRowCoreValid cacheRowNew
            && cacheRowNew.EntityDeltaCode = DEL.ToString()
            && (cacheRowNew.EntityDataCurrent = null && isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
            && isCacheActionUpdate 
        )
    | None -> Assert.True(false)

[<Fact>]
let ``ProcessNonDeleteCacheEntryAsDelete-FromInvalid`` () =
    let deltaSnapshotCacheRowBaseInvalid = { ApiTest.deltaSnapshotCacheRowToCur (deltaSnapshotCacheRowBaseAdd ()) runIdValuePrev with EntityDeltaCode = deltaCodeInvalid }
    match testProcessNonDeleteCacheEntryAsDelete deltaSnapshotCacheRowBaseInvalid with
    | Some (_, _) -> Assert.True(false)
    | None -> Assert.True(true)

[<Fact>]
let ``ProcessNonDeleteCacheEntryAsDelete-FromDel`` () =
    let deltaSnapshotCacheRowBaseDel = deltaSnapshotCacheRowBaseDel ()
    match testProcessNonDeleteCacheEntryAsDelete deltaSnapshotCacheRowBaseDel with
    | Some (_, _) -> Assert.True(false)
    | None -> Assert.True(true)

/////////////////////////////
// MessageOfCacheRowPersisted 10
[<Fact>]
let ``MessageOfCacheRowPersisted-DeltasOnly-InvalidDeltaState`` () =
    let isAll = false
    match ApiTest.testMessageOfCacheRowPersisted isAll { deltaSnapshotCacheRowBaseAdd () with EntityDeltaCode = deltaCodeInvalid } with
    | Some (_) -> Assert.True(false)
    | None -> Assert.True(true)

[<Fact>]
let ``MessageOfCacheRowPersisted-All-InvalidDeltaState`` () =
    let isAll = true
    match ApiTest.testMessageOfCacheRowPersisted isAll { deltaSnapshotCacheRowBaseAdd () with EntityDeltaCode = deltaCodeInvalid } with
    | Some (_) -> Assert.True(false)
    | None -> Assert.True(true)

[<Fact>]
let ``MessageOfCacheRowPersisted-DeltasOnly-Cur`` () =
    let isAll = false
    match ApiTest.testMessageOfCacheRowPersisted isAll (deltaSnapshotCacheRowBaseCur ()) with
    | Some (_) -> Assert.True(false)
    | None -> Assert.True(true)

[<Fact>]
let ``MessageOfCacheRowPersisted-All-Cur`` () =
    let (isAll, cacheRow) = (true, deltaSnapshotCacheRowBaseCur ())
    match ApiTest.testMessageOfCacheRowPersisted isAll cacheRow with
    | Some message -> Assert.True(isEqualMessageAndCacheRow isAll (message, cacheRow))
    | None -> Assert.True(false)

[<Fact>]
let ``MessageOfCacheRowPersisted-DeltasOnly-Add`` () =
    let (isAll, cacheRow) = (false, deltaSnapshotCacheRowBaseAdd ())
    match ApiTest.testMessageOfCacheRowPersisted isAll cacheRow with
    | Some message -> Assert.True(isEqualMessageAndCacheRow isAll (message, cacheRow))
    | None -> Assert.True(false)

[<Fact>]
let ``MessageOfCacheRowPersisted-All-Add`` () =
    let (isAll, cacheRow) = (true, deltaSnapshotCacheRowBaseAdd ())
    match ApiTest.testMessageOfCacheRowPersisted isAll cacheRow with
    | Some message -> Assert.True(isEqualMessageAndCacheRow isAll (message, cacheRow))
    | None -> Assert.True(false)

[<Fact>]
let ``MessageOfCacheRowPersisted-DeltasOnly-Upd`` () =
    let (isAll, cacheRow) = (false, deltaSnapshotCacheRowBaseUpd ())
    match ApiTest.testMessageOfCacheRowPersisted isAll cacheRow with
    | Some message -> Assert.True(isEqualMessageAndCacheRow isAll (message, cacheRow))
    | None -> Assert.True(false)

[<Fact>]
let ``MessageOfCacheRowPersisted-All-Upd`` () =
    let (isAll, cacheRow) = (true, deltaSnapshotCacheRowBaseUpd ())
    match ApiTest.testMessageOfCacheRowPersisted isAll cacheRow with
    | Some message -> Assert.True(isEqualMessageAndCacheRow isAll (message, cacheRow))
    | None -> Assert.True(false)

[<Fact>]
let ``MessageOfCacheRowPersisted-DeltasOnly-Del`` () =
    let (isAll, cacheRow) = (false, deltaSnapshotCacheRowBaseDel ())
    match ApiTest.testMessageOfCacheRowPersisted isAll cacheRow with
    | Some message -> Assert.True(isEqualMessageAndCacheRow isAll (message, cacheRow))
    | None -> Assert.True(false)

[<Fact>]
let ``MessageOfCacheRowPersisted-All-Del`` () =
    let (isAll, cacheRow) = (true, deltaSnapshotCacheRowBaseDel ())
    match ApiTest.testMessageOfCacheRowPersisted isAll cacheRow with
    | Some message -> Assert.True(isEqualMessageAndCacheRow isAll (message, cacheRow))
    | None -> Assert.True(false)

#endif