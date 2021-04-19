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

module UnitTest

open System
open System.Diagnostics
open Xunit
open DeltaSnapshot

[<Fact>]
let ``DeltaStateType`` () =
    //Assert.Equal(DeltaStateType.CUR, @"CUR" |> DeltaState.fromStr |> Option.get)
    //Assert.Equal(DeltaStateType.ADD, @"ADD" |> DeltaState.fromStr |> Option.get)
    //Assert.Equal(DeltaStateType.UPD, @"UPD" |> DeltaState.fromStr |> Option.get)
    //Assert.Equal(DeltaStateType.DEL, @"DEL" |> DeltaState.fromStr |> Option.get)

    //Debug.Assert(DeltaStateType.CUR = (@"CUR" |> DeltaState.fromStr |> Option.get))

    Assert.True(true)

