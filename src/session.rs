// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
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

use std::fmt::Display;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::net::PrivateMsgs;
use constellation_common::net::SharedMsgs;
use constellation_common::sync::Notify;
use constellation_component_common::PartyStreamIdx;

pub trait MulticastClientSession<Prin>: Sized {
    type Config;
    type CreateError: Display;
    type Msg: Clone + Send;
    type Msgs: Clone + SharedMsgs<PartyStreamIdx, Self::Msg> + Send;
    type Recv: Clone + AuthNMsgRecv<Prin, Self::Msg> + Send;
    type Cleanup: ClientSessionCleanup;

    fn create(
        config: Self::Config
    ) -> Result<(Self, Self::Msgs, Notify, Self::Recv), Self::CreateError>;

    fn start(
        self,
        parties: Vec<Prin>
    ) -> Self::Cleanup;
}

pub trait UnicastClientSession<Prin>: Sized {
    type Config;
    type CreateError: Display;
    type Msg: Clone + Send;
    type Msgs: Clone + PrivateMsgs<Self::Msg> + Send;
    type Recv: Clone + AuthNMsgRecv<Prin, Self::Msg> + Send;
    type Cleanup: ClientSessionCleanup;

    fn create(
        config: Self::Config
    ) -> Result<(Self, Self::Msgs, Notify, Self::Recv), Self::CreateError>;

    fn start(self) -> Self::Cleanup;
}

pub trait ClientSessionCleanup {
    fn cleanup(self);
}
