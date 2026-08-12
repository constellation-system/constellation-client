// Copyright © 2024-26 The Johns Hopkins Applied Physics Laboratory LLC.
//
// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.  If you
// would like to purchase a commercial license for this software, please
// contact APL’s Tech Transfer at 240-592-0817 or
// techtransfer@jhuapl.edu.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program.  If not, see
// <https://www.gnu.org/licenses/>.

use std::fmt::Debug;
use std::fmt::Display;

use constellation_component_common::PartyStreamIdx;
use constellation_streams::large_obj::LargeObjProtoTypes;

pub trait MulticastClientSession<Types, InMsg, OutMsg, Args>: Sized
where Types: LargeObjProtoTypes<InMsg, OutMsg>
{
    type Config;
    type CreateError: Debug + Display;
    type StartError: Debug + Display;
    type Cleanup: ClientSessionCleanup;

    fn create(
        config: Self::Config,
        args: Args
    ) -> Result<(Self, Types::Recv, Types::Msgs), Self::CreateError>;

    fn start<I>(
        self,
        parties: I
    ) -> Result<Self::Cleanup, Self::StartError>
    where
        I: Iterator<Item = (PartyStreamIdx, Types::Prin)>;
}

pub trait UnicastClientSession<Types, InMsg, OutMsg, Args>: Sized
where Types: LargeObjProtoTypes<InMsg, OutMsg>
{
    type Config;
    type CreateError: Debug + Display;
    type StartError: Display;
    type Cleanup: ClientSessionCleanup;

    fn create(
        config: Self::Config,
        args: Args
    ) -> Result<(Self, Types::Recv, Types::Msgs), Self::CreateError>;

    fn start(self) -> Result<Self::Cleanup, Self::StartError>;
}

pub trait ClientSessionCleanup {
    fn cleanup(self);
}
