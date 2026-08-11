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
use std::hash::Hash;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::MsgAuthN;
use constellation_common::config::CreateWithParam;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::sync::Notify;
use constellation_component_common::PartyStreamIdx;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::large_obj::LargeObjProtoTypes;
use constellation_streams::multicast::StreamMulticasterFrags;

pub trait ClientSessionTypes {
    type InMsg;
    type OutMsg;
    type SessionPrin: Clone + Debug + Display + Eq + Hash;
    type ProtoTypes: LargeObjProtoTypes<
        Self::InMsg,
        Self::OutMsg,
        SessionPrin = Self::SessionPrin
    >;
}

pub trait MulticastClientSession<Args, Prin>: CreateWithParam<Args> + Sized {
    type StartError: Display;
    type Cleanup: ClientSessionCleanup;

    fn start<I>(
        self,
        parties: I
    ) -> Result<Self::Cleanup, Self::StartError>
    where
        I: Iterator<Item = (PartyStreamIdx, Prin)>;
}

pub trait UnicastClientSession<Args, Prin>: CreateWithParam<Args> + Sized {
    type StartError: Display;
    type Cleanup: ClientSessionCleanup;

    fn start<I>(
        self,
        parties: I
    ) -> Result<Self::Cleanup, Self::StartError>
    where
        I: Iterator<Item = (PartyStreamIdx, Prin)>;
}

pub trait ClientSessionCleanup {
    fn cleanup(self);
}
