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
module TestFunc =
    let isEqualByValue (entity1: Entity) (entity2: Entity) = 
        if (entity1 = null || entity2 = null) then
            false
        else 
            (entity1:>IDataSetEntity).Identifier = (entity2:>IDataSetEntity).Identifier && entity1.PropertyString = entity2.PropertyString 
    let runIdValuePrev = 1L
    let runIdValueCurr = 2L
    let subscriptionDataSetIdValue = 88
    let entityPrev      = Entity ("1", "PREV")
    let entityCurr      = Entity ("1", "CURR")
    let entityChange    = Entity ("1", "CHANGE")
    let isEqualDelegate = IsEqualByValueDelegate (isEqualByValue)
    let isCacheRowCoreValid (cacheRow: DeltaSnapshotCacheRowType<'TCachePrimaryKey, 'TEntity>) = 
        cacheRow.RunId = runIdValueCurr && cacheRow.SubscriptionDataSetId = subscriptionDataSetIdValue && cacheRow.EntityIdentifier = (entityCurr:>IDataSetEntity).Identifier
    let deltaSnapshotCacheRowBaseAdd = 
        fun () -> ApiTest.deltaSnapshotCacheRowCreateAdd runIdValuePrev subscriptionDataSetIdValue entityCurr
    let testProcessDataSetEntityDataSetRun (entity, cacheRow) = 
        ApiTest.testProcessDataSetEntity<System.Int64, Entity> (runIdValueCurr, subscriptionDataSetIdValue) (isEqualDelegate) (entity, cacheRow)

#if DEBUG
[<Fact>]
let ``DeltaStateType`` () =
    Assert.Equal(DeltaStateType.CUR, @"CUR" |> ApiTest.deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.ADD, @"ADD" |> ApiTest.deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.UPD, @"UPD" |> ApiTest.deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.DEL, @"DEL" |> ApiTest.deltaStateFromStr |> Option.get)

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
    let deltaSnapshotCacheRowBaseDel = ApiTest.deltaSnapshotCacheRowToDel (deltaSnapshotCacheRowBaseAdd ()) runIdValuePrev
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, Some deltaSnapshotCacheRowBaseDel) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = ADD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-ReAddFromDelIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseDel = ApiTest.deltaSnapshotCacheRowToDel (deltaSnapshotCacheRowBaseAdd ()) runIdValuePrev
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityChange, Some deltaSnapshotCacheRowBaseDel) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = ADD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityChange) && cacheRowNew.EntityDataPrevious = null
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromUpdIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseUpd = { deltaSnapshotCacheRowBaseAdd () with EntityDeltaCode = UPD.ToString(); EntityDataPrevious = entityPrev; EntityDataCurrent = entityCurr }
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityChange, Some deltaSnapshotCacheRowBaseUpd) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityChange) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromAddIsNotEqual`` () =
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityChange, Some (deltaSnapshotCacheRowBaseAdd ())) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityChange) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
        && not isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromCurIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseCur = ApiTest.deltaSnapshotCacheRowToCur (deltaSnapshotCacheRowBaseAdd ()) runIdValuePrev
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityChange, Some deltaSnapshotCacheRowBaseCur) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityChange) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
        && isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-UpdFromInvalidIsNotEqual`` () =
    let deltaSnapshotCacheRowBaseInvalid = { (deltaSnapshotCacheRowBaseAdd ()) with EntityDeltaCode = "XYZ" }
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityChange, Some deltaSnapshotCacheRowBaseInvalid) 
    Assert.True (isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = UPD.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityChange) && (isEqualByValue cacheRowNew.EntityDataPrevious entityCurr)
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
    let deltaSnapshotCacheRowBaseUpd = { deltaSnapshotCacheRowBaseAdd () with EntityDeltaCode = UPD.ToString(); EntityDataPrevious = entityPrev; EntityDataCurrent = entityCurr }
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
    Assert.True ( isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = CUR.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && isCacheActionUpdate
    )

[<Fact>]
let ``ProcessDataSetEntity-CurFromInvalidIsEqual`` () =
    let deltaSnapshotCacheRowBaseInvalid = { (deltaSnapshotCacheRowBaseAdd ()) with EntityDeltaCode = "XYZ" }
    let (cacheRowNew, isCacheActionUpdate) = testProcessDataSetEntityDataSetRun (entityCurr, Some deltaSnapshotCacheRowBaseInvalid) 
    Assert.True ( isCacheRowCoreValid cacheRowNew
        && cacheRowNew.EntityDeltaCode = CUR.ToString()
        && (isEqualByValue cacheRowNew.EntityDataCurrent entityCurr) && cacheRowNew.EntityDataPrevious = null
        && isCacheActionUpdate
    )
#endif