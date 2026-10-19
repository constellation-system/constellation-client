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

use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::io::Error;
use std::marker::PhantomData;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::config::CreateWithParam;
use constellation_common::error::MutexPoison;
use constellation_component_common::bus::multicast::MulticastLargeObjBus;
use constellation_component_common::bus::multicast::MulticastLargeObjBusCreateError;
use constellation_component_common::bus::types::MulticastLargeObjBusTypes;
use constellation_component_common::config::MulticastLargeObjBusConfig;
use constellation_component_common::config::XactBatchCodecConfig;
use constellation_component_common::xact::XactBatchCodecCreateError;
use constellation_component_common::xact::XactCommittedRoundCodecCreateError;
use constellation_component_common::xact::XactNotifyCodecCreateError;
use constellation_component_common::xact::XactReqCodecCreateError;
use constellation_component_common::xact::XactSealedCodecCreateError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use log::info;
use serde::Deserialize;
use serde::Serialize;

use crate::component::ClientComponentTypes;
use crate::component::HashClientComponentTypes;
use crate::config::MulticastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::MulticastClientSession;

pub trait MulticastClientComponentTypes<Types>
where Types: ClientComponentTypes {
    type SessionPrin: Clone + Debug + Display + Eq + Hash + Send + Sync + for<'a> Deserialize<'a> + Serialize;
    type IDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type IDsCreateError: Debug + Display;
    type ResolverConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type EpochsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type SessionAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type MsgAuthConfig:  for<'a> Deserialize<'a> + Serialize;
    type LargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type UnixConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type UDPConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type MsgAuthCreateError: Debug + Display;
    type Msgs: 'static + Send;
    type Recv: 'static + AuthNMsgRecv<Types::MsgPrin, Types::AuthNMsg> + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionParam;
    type SessionConfig: for<'a> Deserialize<'a> + Serialize;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: MulticastClientSession<
            Self::SessionPrin,
            Self::Recv,
            Self::Msgs,
            Cleanup = Self::SessionCleanup,
            Config = Self::SessionConfig,
            Param = Self::SessionParam,
            CreateError = Self::SessionCreateError,
            StartError = Self::SessionStartError
        >;
    type BusTypes: MulticastLargeObjBusTypes<
            Self::Ctx,
            SessionPrin = Self::SessionPrin,
            Msgs = Self::Msgs,
            Recv = Self::Recv,
            EpochsConfig = Self::EpochsConfig,
            UnixConfig = Self::UnixConfig,
            UDPConfig = Self::UDPConfig,
            HashConfig = Types::HashConfig,
            HashCreateError = Types::HashCreateError,
            ResolverConfig = Self::ResolverConfig,
            SessionAuthConfig = Self::SessionAuthConfig,
            LargeObjMsgAuthConfig = Self::LargeObjMsgAuthConfig,
            MsgAuthConfig = Self::MsgAuthConfig,
            MsgAuthCreateError = Self::MsgAuthCreateError,
            DecoderConfig = XactBatchCodecConfig<
                Types::SealCodecConfig,
                Types::PayloadCodecConfig,
                Types::EffectCodecConfig,
                Types::ResultCodecConfig,
                Types::ErrorCodecConfig
            >,
            DecoderCreateError = XactBatchCodecCreateError<
                XactSealedCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactCommittedRoundCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactNotifyCodecCreateError<
                    Types::ResultCodecCreateError,
                    Types::ErrorCodecCreateError
                >
            >,
            EncoderConfig = XactBatchCodecConfig<
                Types::SealCodecConfig,
                Types::PayloadCodecConfig,
                Types::EffectCodecConfig,
                Types::ResultCodecConfig,
                Types::ErrorCodecConfig
            >,
            EncoderCreateError = XactBatchCodecCreateError<
                XactSealedCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactCommittedRoundCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactNotifyCodecCreateError<
                    Types::ResultCodecCreateError,
                    Types::ErrorCodecCreateError
                >
            >,
            IDsConfig = Self::IDsConfig,
            IDsCreateError = Self::IDsCreateError
        > + Send;
}

pub trait HashMulticastClientComponentTypes<Types>
where Types: HashClientComponentTypes {
    type SessionPrin: Clone + Debug + Display + Eq + Hash + Send + Sync + for<'a> Deserialize<'a> + Serialize;
    type IDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type IDsCreateError: Debug + Display;
    type ResolverConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type EpochsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type SessionAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type MsgAuthConfig:  for<'a> Deserialize<'a> + Serialize;
    type LargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type UnixConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type UDPConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type MsgAuthCreateError: Debug + Display;
    type Msgs: 'static + Send;
    type Recv: 'static + AuthNMsgRecv<Types::MsgPrin, Types::AuthNMsg> + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionParam;
    type SessionConfig: for<'a> Deserialize<'a> + Serialize;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: MulticastClientSession<
            Self::SessionPrin,
            Self::Recv,
            Self::Msgs,
            Cleanup = Self::SessionCleanup,
            Config = Self::SessionConfig,
            Param = Self::SessionParam,
            CreateError = Self::SessionCreateError,
            StartError = Self::SessionStartError
        >;
    type BusTypes: MulticastLargeObjBusTypes<
            Self::Ctx,
            SessionPrin = Self::SessionPrin,
            Msgs = Self::Msgs,
            Recv = Self::Recv,
            EpochsConfig = Self::EpochsConfig,
            UnixConfig = Self::UnixConfig,
            UDPConfig = Self::UDPConfig,
            HashConfig = Types::HashConfig,
            HashCreateError = Types::HashCreateError,
            ResolverConfig = Self::ResolverConfig,
            SessionAuthConfig = Self::SessionAuthConfig,
            LargeObjMsgAuthConfig = Self::LargeObjMsgAuthConfig,
            MsgAuthConfig = Self::MsgAuthConfig,
            MsgAuthCreateError = Self::MsgAuthCreateError,
            DecoderConfig = XactBatchCodecConfig<
                Types::SealCodecConfig,
                Types::PayloadCodecConfig,
                Types::EffectCodecConfig,
                Types::ResultCodecConfig,
                Types::ErrorCodecConfig
            >,
            DecoderCreateError = XactBatchCodecCreateError<
                XactSealedCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactCommittedRoundCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactNotifyCodecCreateError<
                    Types::ResultCodecCreateError,
                    Types::ErrorCodecCreateError
                >
            >,
            EncoderConfig = XactBatchCodecConfig<
                Types::SealCodecConfig,
                Types::PayloadCodecConfig,
                Types::EffectCodecConfig,
                Types::ResultCodecConfig,
                Types::ErrorCodecConfig
            >,
            EncoderCreateError = XactBatchCodecCreateError<
                XactSealedCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactCommittedRoundCodecCreateError<
                    Types::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Types::PayloadCodecCreateError,
                        Types::EffectCodecCreateError
                    >
                >,
                XactNotifyCodecCreateError<
                    Types::ResultCodecCreateError,
                    Types::ErrorCodecCreateError
                >
            >,
            IDsConfig = Self::IDsConfig,
            IDsCreateError = Self::IDsCreateError
        > + Send;
}

pub struct SimpleMulticastClientComponentTypes<ClientTypes, BusTypes, Session, Ctx>
where Ctx: 'static + NSNameCachesCtx + Send
{
    client_types: PhantomData<ClientTypes>,
    bus_types: PhantomData<BusTypes>,
    session: PhantomData<Session>,
    ctx: PhantomData<Ctx>
}

pub struct SimpleHashMulticastClientComponentTypes<ClientTypes, BusTypes, Session, Ctx>
where Ctx: 'static + NSNameCachesCtx + Send
{
    client_types: PhantomData<ClientTypes>,
    bus_types: PhantomData<BusTypes>,
    session: PhantomData<Session>,
    ctx: PhantomData<Ctx>
}

pub struct MulticastClientComponent<ClientTypes, Types>
where
    ClientTypes: ClientComponentTypes,
    Types: MulticastClientComponentTypes<ClientTypes> {
    config: MulticastLargeObjBusConfig<
        Types::SessionPrin,
        ClientTypes::HashConfig,
        Types::SessionAuthConfig,
        Types::LargeObjMsgAuthConfig,
        Types::UnixConfig,
        Types::UDPConfig,
        XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        Types::IDsConfig,
        Types::ResolverConfig,
        Types::EpochsConfig,
        Types::MsgAuthConfig
    >,
    session_param: Types::SessionParam,
    session_config: Types::SessionConfig,
    ctx: Types::Ctx
}

pub struct HashMulticastClientComponent<ClientTypes, Types>
where
    ClientTypes: HashClientComponentTypes,
    Types: HashMulticastClientComponentTypes<ClientTypes> {
    config: MulticastLargeObjBusConfig<
        Types::SessionPrin,
        ClientTypes::HashConfig,
        Types::SessionAuthConfig,
        Types::LargeObjMsgAuthConfig,
        Types::UnixConfig,
        Types::UDPConfig,
        XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        Types::IDsConfig,
        Types::ResolverConfig,
        Types::EpochsConfig,
        Types::MsgAuthConfig
    >,
    session_param: Types::SessionParam,
    session_config: Types::SessionConfig,
    ctx: Types::Ctx
}

pub struct MulticastClientComponentCleanup<ClientTypes, Types>
where
    ClientTypes: ClientComponentTypes,
    Types: MulticastClientComponentTypes<ClientTypes> {
    multicast: MulticastLargeObjBus<Types::BusTypes, Types::Ctx>,
    session: Types::SessionCleanup
}

pub struct HashMulticastClientComponentCleanup<ClientTypes, Types>
where
    ClientTypes: HashClientComponentTypes,
    Types: HashMulticastClientComponentTypes<ClientTypes> {
    multicast: MulticastLargeObjBus<Types::BusTypes, Types::Ctx>,
    session: Types::SessionCleanup
}

pub struct MulticastClientParam<Ctx, Param> {
    session_param: Param,
    ctx: Ctx
}

#[derive(Debug)]
pub enum MulticastClientComponentRunError<Session, Multicast, Start> {
    Session { err: Session },
    Multicast { err: Multicast },
    Start { err: Start },
    IO { err: Error },
    SkippedIdx
}

impl<ClientTypes, BusTypes, Session, Ctx>
    MulticastClientComponentTypes<ClientTypes>
    for SimpleMulticastClientComponentTypes<ClientTypes, BusTypes, Session, Ctx>
where
    ClientTypes: ClientComponentTypes,
    BusTypes: MulticastLargeObjBusTypes<
        Ctx,
        MsgPrin = ClientTypes::MsgPrin,
        AuthNMsg = ClientTypes::AuthNMsg,
        HashConfig = ClientTypes::HashConfig,
        HashCreateError = ClientTypes::HashCreateError,
        DecoderConfig = XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        DecoderCreateError = XactBatchCodecCreateError<
            XactSealedCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactCommittedRoundCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactNotifyCodecCreateError<
                ClientTypes::ResultCodecCreateError,
                ClientTypes::ErrorCodecCreateError
            >
        >,
        EncoderConfig = XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        EncoderCreateError = XactBatchCodecCreateError<
            XactSealedCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactCommittedRoundCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactNotifyCodecCreateError<
                ClientTypes::ResultCodecCreateError,
                ClientTypes::ErrorCodecCreateError
            >
        >,
    > + Send,
    BusTypes::SessionPrin: for<'a> Deserialize<'a> + Serialize,
    BusTypes::IDsConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::ResolverConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::EpochsConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::SessionAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::MsgAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::LargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::UnixConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::UDPConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::Msgs: 'static,
    BusTypes::Recv: 'static,
    Session: MulticastClientSession<
        BusTypes::SessionPrin,
        BusTypes::Recv,
        BusTypes::Msgs,
    >,
    Session::Config: for<'a> Deserialize<'a> + Serialize,
    Ctx: 'static + NSNameCachesCtx + Send
{
    type SessionPrin = BusTypes::SessionPrin;
    type IDsConfig = BusTypes::IDsConfig;
    type IDsCreateError = BusTypes::IDsCreateError;
    type ResolverConfig = BusTypes::ResolverConfig;
    type EpochsConfig = BusTypes::EpochsConfig;
    type SessionAuthConfig = BusTypes::SessionAuthConfig;
    type MsgAuthConfig = BusTypes::MsgAuthConfig;
    type LargeObjMsgAuthConfig = BusTypes::LargeObjMsgAuthConfig;
    type UnixConfig = BusTypes::UnixConfig;
    type UDPConfig = BusTypes::UDPConfig;
    type MsgAuthCreateError = BusTypes::MsgAuthCreateError;
    type Msgs = BusTypes::Msgs;
    type Recv = BusTypes::Recv;
    type Ctx = Ctx;
    type SessionParam = Session::Param;
    type SessionConfig = Session::Config;
    type SessionCleanup = Session::Cleanup;
    type SessionCreateError = Session::CreateError;
    type SessionStartError = Session::StartError;
    type Session = Session;
    type BusTypes = BusTypes;
}

impl<Ctx, Param> MulticastClientParam<Ctx, Param> {
    #[inline]
    pub fn new(
        session_param: Param,
        ctx: Ctx
    ) -> Self {
        MulticastClientParam {
            session_param: session_param,
            ctx: ctx
        }
    }
}

impl<ClientTypes, BusTypes, Session, Ctx>
    HashMulticastClientComponentTypes<ClientTypes>
    for SimpleHashMulticastClientComponentTypes<ClientTypes, BusTypes, Session, Ctx>
where
    ClientTypes: HashClientComponentTypes,
    BusTypes: MulticastLargeObjBusTypes<
        Ctx,
        MsgPrin = ClientTypes::MsgPrin,
        AuthNMsg = ClientTypes::AuthNMsg,
        HashConfig = ClientTypes::HashConfig,
        HashCreateError = ClientTypes::HashCreateError,
        DecoderConfig = XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        DecoderCreateError = XactBatchCodecCreateError<
            XactSealedCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactCommittedRoundCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactNotifyCodecCreateError<
                ClientTypes::ResultCodecCreateError,
                ClientTypes::ErrorCodecCreateError
            >
        >,
        EncoderConfig = XactBatchCodecConfig<
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig
        >,
        EncoderCreateError = XactBatchCodecCreateError<
            XactSealedCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactCommittedRoundCodecCreateError<
                ClientTypes::SealCodecCreateError,
                XactReqCodecCreateError<
                    ClientTypes::PayloadCodecCreateError,
                    ClientTypes::EffectCodecCreateError
                >
            >,
            XactNotifyCodecCreateError<
                ClientTypes::ResultCodecCreateError,
                ClientTypes::ErrorCodecCreateError
            >
        >,
    > + Send,
    BusTypes::SessionPrin: for<'a> Deserialize<'a> + Serialize,
    BusTypes::IDsConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::ResolverConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::EpochsConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::SessionAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::MsgAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::LargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::UnixConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::UDPConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::Msgs: 'static,
    BusTypes::Recv: 'static,
    Session: MulticastClientSession<
        BusTypes::SessionPrin,
        BusTypes::Recv,
        BusTypes::Msgs,
    >,
    Session::Config: for<'a> Deserialize<'a> + Serialize,
    Ctx: 'static + NSNameCachesCtx + Send
{
    type SessionPrin = BusTypes::SessionPrin;
    type IDsConfig = BusTypes::IDsConfig;
    type IDsCreateError = BusTypes::IDsCreateError;
    type ResolverConfig = BusTypes::ResolverConfig;
    type EpochsConfig = BusTypes::EpochsConfig;
    type SessionAuthConfig = BusTypes::SessionAuthConfig;
    type MsgAuthConfig = BusTypes::MsgAuthConfig;
    type LargeObjMsgAuthConfig = BusTypes::LargeObjMsgAuthConfig;
    type UnixConfig = BusTypes::UnixConfig;
    type UDPConfig = BusTypes::UDPConfig;
    type MsgAuthCreateError = BusTypes::MsgAuthCreateError;
    type Msgs = BusTypes::Msgs;
    type Recv = BusTypes::Recv;
    type Ctx = Ctx;
    type SessionParam = Session::Param;
    type SessionConfig = Session::Config;
    type SessionCleanup = Session::Cleanup;
    type SessionCreateError = Session::CreateError;
    type SessionStartError = Session::StartError;
    type Session = Session;
    type BusTypes = BusTypes;
}

impl<ClientTypes, Types>
    CreateWithParam<MulticastClientParam<Types::Ctx, Types::SessionParam>>
    for MulticastClientComponent<ClientTypes, Types>
where
    ClientTypes: ClientComponentTypes,
    Types: MulticastClientComponentTypes<ClientTypes>
{
    type Config = MulticastClientConfig<
        Types::SessionPrin,
        Types::SessionConfig,
        ClientTypes::SealCodecConfig,
        ClientTypes::PayloadCodecConfig,
        ClientTypes::EffectCodecConfig,
        ClientTypes::ResultCodecConfig,
        ClientTypes::ErrorCodecConfig,
        ClientTypes::HashConfig,
        Types::SessionAuthConfig,
        Types::LargeObjMsgAuthConfig,
        Types::UnixConfig,
        Types::UDPConfig,
        Types::IDsConfig,
        Types::ResolverConfig,
        Types::EpochsConfig,
        Types::MsgAuthConfig
    >;
    type CreateError = Infallible;

    fn create(
        config: MulticastClientConfig<
            Types::SessionPrin,
            Types::SessionConfig,
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig,
            ClientTypes::HashConfig,
            Types::SessionAuthConfig,
            Types::LargeObjMsgAuthConfig,
            Types::UnixConfig,
            Types::UDPConfig,
            Types::IDsConfig,
            Types::ResolverConfig,
            Types::EpochsConfig,
            Types::MsgAuthConfig
        >,
        param: MulticastClientParam<Types::Ctx, Types::SessionParam>
    ) -> Result<Self, Self::CreateError> {
        let (config, session_config) = config.take();
        let MulticastClientParam { ctx, session_param } = param;

        Ok(MulticastClientComponent {
            config: config,
            session_param: session_param,
            session_config: session_config,
            ctx: ctx
        })
    }
}

impl<ClientTypes, Types>
    CreateWithParam<MulticastClientParam<Types::Ctx, Types::SessionParam>>
    for HashMulticastClientComponent<ClientTypes, Types>
where
    ClientTypes: HashClientComponentTypes,
    Types: HashMulticastClientComponentTypes<ClientTypes>
{
    type Config = MulticastClientConfig<
        Types::SessionPrin,
        Types::SessionConfig,
        ClientTypes::SealCodecConfig,
        ClientTypes::PayloadCodecConfig,
        ClientTypes::EffectCodecConfig,
        ClientTypes::ResultCodecConfig,
        ClientTypes::ErrorCodecConfig,
        ClientTypes::HashConfig,
        Types::SessionAuthConfig,
        Types::LargeObjMsgAuthConfig,
        Types::UnixConfig,
        Types::UDPConfig,
        Types::IDsConfig,
        Types::ResolverConfig,
        Types::EpochsConfig,
        Types::MsgAuthConfig
    >;
    type CreateError = Infallible;

    fn create(
        config: MulticastClientConfig<
            Types::SessionPrin,
            Types::SessionConfig,
            ClientTypes::SealCodecConfig,
            ClientTypes::PayloadCodecConfig,
            ClientTypes::EffectCodecConfig,
            ClientTypes::ResultCodecConfig,
            ClientTypes::ErrorCodecConfig,
            ClientTypes::HashConfig,
            Types::SessionAuthConfig,
            Types::LargeObjMsgAuthConfig,
            Types::UnixConfig,
            Types::UDPConfig,
            Types::IDsConfig,
            Types::ResolverConfig,
            Types::EpochsConfig,
            Types::MsgAuthConfig
        >,
        param: MulticastClientParam<Types::Ctx, Types::SessionParam>
    ) -> Result<Self, Self::CreateError> {
        let (config, session_config) = config.take();
        let MulticastClientParam { ctx, session_param } = param;

        Ok(HashMulticastClientComponent {
            config: config,
            session_param: session_param,
            session_config: session_config,
            ctx: ctx
        })
    }
}

impl<ClientTypes, Types> MulticastClientComponent<ClientTypes, Types>
where
    Types: 'static + MulticastClientComponentTypes<ClientTypes>,
    Types::BusTypes: 'static,
    ClientTypes: ClientComponentTypes
{
    pub fn start(
        self
    ) -> Result<
        MulticastClientComponentCleanup<ClientTypes, Types>,
        MulticastClientComponentRunError<
            Types::SessionCreateError,
            MulticastLargeObjBusCreateError<
                ClientTypes::HashCreateError,
                Types::MsgAuthCreateError,
                LargeObjProtoCreateError<
                    XactBatchCodecCreateError<
                        XactSealedCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactCommittedRoundCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactNotifyCodecCreateError<
                            ClientTypes::ResultCodecCreateError,
                            ClientTypes::ErrorCodecCreateError
                        >
                    >,
                    XactBatchCodecCreateError<
                        XactSealedCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactCommittedRoundCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactNotifyCodecCreateError<
                            ClientTypes::ResultCodecCreateError,
                            ClientTypes::ErrorCodecCreateError
                        >
                    >,
                    Types::IDsCreateError
                >,
                MutexPoison
            >,
            Types::SessionStartError
        >
    > {
        let MulticastClientComponent {
            session_param,
            session_config,
            config,
            ctx,
            ..
        } = self;

        info!(target: "multicast-client-component",
              "starting multicast client component");

        let (session, recv, msgs) =
            Types::Session::create(session_config, session_param).map_err(
                |err| MulticastClientComponentRunError::Session { err: err }
            )?;
        let (multicast, parties) =
            MulticastLargeObjBus::start(config, ctx, recv, msgs).map_err(
                |err| MulticastClientComponentRunError::Multicast { err: err }
            )?;
        let session_cleanup =
            session.start(parties.into_iter()).map_err(|err| {
                MulticastClientComponentRunError::Start { err: err }
            })?;

        Ok(MulticastClientComponentCleanup {
            multicast: multicast,
            session: session_cleanup
        })
    }
}

impl<ClientTypes, Types> HashMulticastClientComponent<ClientTypes, Types>
where
    Types: 'static + HashMulticastClientComponentTypes<ClientTypes>,
    Types::BusTypes: 'static,
    ClientTypes: HashClientComponentTypes
{
    pub fn start(
        self
    ) -> Result<
        HashMulticastClientComponentCleanup<ClientTypes, Types>,
        MulticastClientComponentRunError<
            Types::SessionCreateError,
            MulticastLargeObjBusCreateError<
                ClientTypes::HashCreateError,
                Types::MsgAuthCreateError,
                LargeObjProtoCreateError<
                    XactBatchCodecCreateError<
                        XactSealedCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactCommittedRoundCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactNotifyCodecCreateError<
                            ClientTypes::ResultCodecCreateError,
                            ClientTypes::ErrorCodecCreateError
                        >
                    >,
                    XactBatchCodecCreateError<
                        XactSealedCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactCommittedRoundCodecCreateError<
                            ClientTypes::SealCodecCreateError,
                            XactReqCodecCreateError<
                                ClientTypes::PayloadCodecCreateError,
                                ClientTypes::EffectCodecCreateError
                            >
                        >,
                        XactNotifyCodecCreateError<
                            ClientTypes::ResultCodecCreateError,
                            ClientTypes::ErrorCodecCreateError
                        >
                    >,
                    Types::IDsCreateError
                >,
                MutexPoison
            >,
            Types::SessionStartError
        >
    > {
        let HashMulticastClientComponent {
            session_param,
            session_config,
            config,
            ctx,
            ..
        } = self;

        info!(target: "multicast-client-component",
              "starting multicast client component");

        let (session, recv, msgs) =
            Types::Session::create(session_config, session_param).map_err(
                |err| MulticastClientComponentRunError::Session { err: err }
            )?;
        let (multicast, parties) =
            MulticastLargeObjBus::start(config, ctx, recv, msgs).map_err(
                |err| MulticastClientComponentRunError::Multicast { err: err }
            )?;
        let session_cleanup =
            session.start(parties.into_iter()).map_err(|err| {
                MulticastClientComponentRunError::Start { err: err }
            })?;

        Ok(HashMulticastClientComponentCleanup {
            multicast: multicast,
            session: session_cleanup
        })
    }
}

impl<ClientTypes, Types> MulticastClientComponentCleanup<ClientTypes, Types>
where
    Types: 'static + MulticastClientComponentTypes<ClientTypes>,
    ClientTypes: 'static + ClientComponentTypes
{
    pub fn cleanup(self) {
        let MulticastClientComponentCleanup { multicast, session } = self;

        multicast.cleanup();
        session.cleanup();
    }
}

impl<ClientTypes, Types> HashMulticastClientComponentCleanup<ClientTypes, Types>
where
    Types: 'static + HashMulticastClientComponentTypes<ClientTypes>,
    ClientTypes: 'static + HashClientComponentTypes
{
    pub fn cleanup(self) {
        let HashMulticastClientComponentCleanup { multicast, session } = self;

        multicast.cleanup();
        session.cleanup();
    }
}

impl<Session, Multicast, Start> Display
    for MulticastClientComponentRunError<Session, Multicast, Start>
where
    Session: Display,
    Multicast: Display,
    Start: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            MulticastClientComponentRunError::Session { err } => err.fmt(f),
            MulticastClientComponentRunError::Multicast { err } => err.fmt(f),
            MulticastClientComponentRunError::Start { err } => err.fmt(f),
            MulticastClientComponentRunError::IO { err } => {
                write!(f, "{}", err)
            }
            MulticastClientComponentRunError::SkippedIdx => {
                write!(f, "stream parties skipped an index")
            }
        }
    }
}
