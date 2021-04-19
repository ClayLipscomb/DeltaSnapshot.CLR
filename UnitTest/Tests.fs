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

#if DEBUG
[<Fact>]
let ``DeltaStateType`` () =
    Assert.Equal(DeltaStateType.CUR, @"CUR" |> deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.ADD, @"ADD" |> deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.UPD, @"UPD" |> deltaStateFromStr |> Option.get)
    Assert.Equal(DeltaStateType.DEL, @"DEL" |> deltaStateFromStr |> Option.get)

    //Assert.Equal(DeltaStateType.CUR.ToString(), @"CUR")
    //Assert.Equal(DeltaStateType.ADD.ToString(), @"ADD")
    //Assert.Equal(DeltaStateType.UPD.ToString(), @"UPD")
    //Assert.Equal(DeltaStateType.DEL.ToString(), @"DEL")

    //Assert.True(         Some DeltaStateType.ADD = DeltaState.fromStr (DeltaStateType.CUR.ToString()))
#endif

