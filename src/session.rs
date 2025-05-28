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
use std::hash::Hash;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::MsgAuthN;
use constellation_common::codec::Codec;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::ids::IDGen;
use constellation_common::sync::Notify;
use constellation_component_common::PartyStreamIdx;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::multicast::StreamMulticasterFrags;

pub trait MulticastClientSession<
    H,
    Msg,
    Wrapper,
    Auth,
    WrapperCodec,
    IDs,
    Msgs,
    Recv
>: Sized where
    Recv: AuthNMsgRecv<Auth::Prin, Msg>,
    Msgs: LargeObjMsgs<H, Wrapper> + Send,
    IDs: IDGen + Iterator<Item = LargeObjID>,
    Auth: MsgAuthN<Msg, Wrapper>,
    WrapperCodec: Clone + Codec<Wrapper>,
    <WrapperCodec as Codec<Wrapper>>::Param: Default,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq {
    type Config;
    type Args;
    type CreateError: Display;
    type StartError: Display;
    type Cleanup: ClientSessionCleanup;

    fn create(
        args: Self::Args,
        config: Self::Config
    ) -> Result<
        (
            Self,
            Notify,
            LargeObjProto<
                H,
                Msg,
                Wrapper,
                Auth,
                PartyStreamIdx,
                WrapperCodec,
                IDs,
                Msgs,
                Recv,
                StreamMulticasterFrags<PartyStreamIdx, OutboundFrags>
            >
        ),
        Self::CreateError
    >;

    fn start<I>(
        self,
        parties: I
    ) -> Result<Self::Cleanup, Self::StartError>
    where
        I: Iterator<Item = (PartyStreamIdx, Auth::SessionPrin)>;
}

pub trait UnicastClientSession<
    H,
    Msg,
    Wrapper,
    Auth,
    WrapperCodec,
    IDs,
    Msgs,
    Recv
>: Sized where
    Recv: AuthNMsgRecv<Auth::Prin, Msg>,
    Msgs: LargeObjMsgs<H, Wrapper> + Send,
    IDs: IDGen + Iterator<Item = LargeObjID>,
    Auth: MsgAuthN<Msg, Wrapper>,
    WrapperCodec: Clone + Codec<Wrapper>,
    <WrapperCodec as Codec<Wrapper>>::Param: Default,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq {
    type Args;
    type Config;
    type CreateError: Display;
    type StartError: Display;
    type Cleanup: ClientSessionCleanup;

    fn create(
        args: Self::Args,
        config: Self::Config
    ) -> Result<
        (
            Self,
            Notify,
            LargeObjProto<
                H,
                Msg,
                Wrapper,
                Auth,
                (),
                WrapperCodec,
                IDs,
                Msgs,
                Recv,
                OutboundFrags
            >
        ),
        Self::CreateError
    >;

    fn start(self) -> Result<Self::Cleanup, Self::StartError>;
}

pub trait ClientSessionCleanup {
    fn cleanup(self);
}
