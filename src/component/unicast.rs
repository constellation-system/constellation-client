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
use std::fmt::Formatter;
use std::io::Error;
use std::marker::PhantomData;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::types::UnicastLargeObjBusTypes;
use constellation_component_common::bus::unicast::UnicastLargeObjBus;
use constellation_component_common::bus::unicast::UnicastLargeObjBusCreateError;
use constellation_component_common::bus::unicast::UnicastLargeObjBusStarter;
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
use crate::config::UnicastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::UnicastClientSession;

pub trait UnicastClientComponentTypes<Types>
where
    Types: ClientComponentTypes {
    type IDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type IDsCreateError: Debug + Display;
    type ResolverConfig: Clone
        + Default
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type EpochsConfig: Clone
        + Default
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type SessionAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type MsgAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type LargeObjMsgAuthConfig: Clone
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type UnixConfig: Clone
        + Default
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type UDPConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type MsgAuthCreateError: Debug + Display;
    type Msgs: 'static + Send;
    type Recv: 'static + AuthNMsgRecv<Types::MsgPrin, Types::AuthNMsg> + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionParam;
    type SessionConfig: for<'a> Deserialize<'a> + Serialize;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: UnicastClientSession<
            Self::Recv,
            Self::Msgs,
            Cleanup = Self::SessionCleanup,
            Config = Self::SessionConfig,
            Param = Self::SessionParam,
            CreateError = Self::SessionCreateError,
            StartError = Self::SessionStartError
        >;
    type BusTypes: UnicastLargeObjBusTypes<
            Self::Ctx,
            MsgPrin = Types::MsgPrin,
            AuthNMsg = Types::AuthNMsg,
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

pub trait HashUnicastClientComponentTypes<Types>
where
    Types: HashClientComponentTypes {
    type IDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type IDsCreateError: Debug + Display;
    type ResolverConfig: Clone
        + Default
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type EpochsConfig: Clone
        + Default
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type SessionAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type MsgAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize + Send;
    type LargeObjMsgAuthConfig: Clone
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type UnixConfig: Clone
        + Default
        + for<'a> Deserialize<'a>
        + Serialize
        + Send;
    type UDPConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize + Send;
    type MsgAuthCreateError: Debug + Display;
    type Msgs: 'static + Send;
    type Recv: 'static + AuthNMsgRecv<Types::MsgPrin, Types::AuthNMsg> + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionParam;
    type SessionConfig: for<'a> Deserialize<'a> + Serialize;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: UnicastClientSession<
            Self::Recv,
            Self::Msgs,
            Cleanup = Self::SessionCleanup,
            Config = Self::SessionConfig,
            Param = Self::SessionParam,
            CreateError = Self::SessionCreateError,
            StartError = Self::SessionStartError
        >;
    type BusTypes: UnicastLargeObjBusTypes<
            Self::Ctx,
            MsgPrin = Types::MsgPrin,
            AuthNMsg = Types::AuthNMsg,
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

pub struct SimpleUnicastClientComponentTypes<
    ClientTypes,
    BusTypes,
    Session,
    Ctx
> where
    Ctx: 'static + NSNameCachesCtx + Send {
    client_types: PhantomData<ClientTypes>,
    bus_types: PhantomData<BusTypes>,
    session: PhantomData<Session>,
    ctx: PhantomData<Ctx>
}

pub struct SimpleHashUnicastClientComponentTypes<
    ClientTypes,
    BusTypes,
    Session,
    Ctx
> where
    Ctx: 'static + NSNameCachesCtx + Send {
    client_types: PhantomData<ClientTypes>,
    bus_types: PhantomData<BusTypes>,
    session: PhantomData<Session>,
    ctx: PhantomData<Ctx>
}

pub struct UnicastClientComponent<ClientTypes, Types>
where
    ClientTypes: 'static + ClientComponentTypes,
    Types: 'static + UnicastClientComponentTypes<ClientTypes> {
    starter: UnicastLargeObjBusStarter<Types::BusTypes, Types::Ctx>,
    session_param: Types::SessionParam,
    session_config: Types::SessionConfig,
    notify: Notify
}

pub struct HashUnicastClientComponent<ClientTypes, Types>
where
    ClientTypes: 'static + HashClientComponentTypes,
    Types: 'static + HashUnicastClientComponentTypes<ClientTypes> {
    starter: UnicastLargeObjBusStarter<Types::BusTypes, Types::Ctx>,
    session_param: Types::SessionParam,
    session_config: Types::SessionConfig,
    notify: Notify
}

pub struct UnicastClientComponentCleanup<ClientTypes, Types>
where
    ClientTypes: ClientComponentTypes,
    Types: UnicastClientComponentTypes<ClientTypes> {
    unicast: UnicastLargeObjBus<Types::BusTypes, Types::Ctx>,
    session: Types::SessionCleanup
}

pub struct HashUnicastClientComponentCleanup<ClientTypes, Types>
where
    ClientTypes: HashClientComponentTypes,
    Types: HashUnicastClientComponentTypes<ClientTypes> {
    unicast: UnicastLargeObjBus<Types::BusTypes, Types::Ctx>,
    session: Types::SessionCleanup
}

pub struct UnicastClientParam<Ctx, Param> {
    session_param: Param,
    ctx: Ctx
}

#[derive(Debug)]
pub enum UnicastClientComponentCreateError<Session> {
    Session { err: Session },
    IO { err: Error }
}

#[derive(Debug)]
pub enum UnicastClientComponentRunError<Session, Unicast, Start> {
    Session { err: Session },
    Unicast { err: Unicast },
    Start { err: Start },
    IO { err: Error },
    SkippedIdx
}

impl<ClientTypes, BusTypes, Session, Ctx>
    UnicastClientComponentTypes<ClientTypes>
    for SimpleUnicastClientComponentTypes<ClientTypes, BusTypes, Session, Ctx>
where
    ClientTypes: ClientComponentTypes,
    BusTypes: UnicastLargeObjBusTypes<
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
            >
        > + Send,
    BusTypes::SessionPrin: for<'a> Deserialize<'a> + Serialize,
    BusTypes::IDsConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::ResolverConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::EpochsConfig: Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::SessionAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::MsgAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::LargeObjMsgAuthConfig:
        Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::UnixConfig: Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::UDPConfig: Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::Msgs: 'static,
    BusTypes::Recv: 'static,
    Session: UnicastClientSession<BusTypes::Recv, BusTypes::Msgs>,
    Session::Config: for<'a> Deserialize<'a> + Serialize,
    Ctx: 'static + NSNameCachesCtx + Send
{
    type BusTypes = BusTypes;
    type Ctx = Ctx;
    type EpochsConfig = BusTypes::EpochsConfig;
    type IDsConfig = BusTypes::IDsConfig;
    type IDsCreateError = BusTypes::IDsCreateError;
    type LargeObjMsgAuthConfig = BusTypes::LargeObjMsgAuthConfig;
    type MsgAuthConfig = BusTypes::MsgAuthConfig;
    type MsgAuthCreateError = BusTypes::MsgAuthCreateError;
    type Msgs = BusTypes::Msgs;
    type Recv = BusTypes::Recv;
    type ResolverConfig = BusTypes::ResolverConfig;
    type Session = Session;
    type SessionAuthConfig = BusTypes::SessionAuthConfig;
    type SessionCleanup = Session::Cleanup;
    type SessionConfig = Session::Config;
    type SessionCreateError = Session::CreateError;
    type SessionParam = Session::Param;
    type SessionStartError = Session::StartError;
    type UDPConfig = BusTypes::UDPConfig;
    type UnixConfig = BusTypes::UnixConfig;
}

impl<ClientTypes, BusTypes, Session, Ctx>
    HashUnicastClientComponentTypes<ClientTypes>
    for SimpleHashUnicastClientComponentTypes<
        ClientTypes,
        BusTypes,
        Session,
        Ctx
    >
where
    ClientTypes: HashClientComponentTypes,
    BusTypes: UnicastLargeObjBusTypes<
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
            >
        > + Send,
    BusTypes::SessionPrin: for<'a> Deserialize<'a> + Serialize,
    BusTypes::IDsConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::ResolverConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::EpochsConfig: Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::SessionAuthConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::MsgAuthConfig: Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::LargeObjMsgAuthConfig:
        Clone + for<'a> Deserialize<'a> + Serialize,
    BusTypes::UnixConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::UDPConfig: for<'a> Deserialize<'a> + Serialize,
    BusTypes::Msgs: 'static,
    BusTypes::Recv: 'static,
    Session: UnicastClientSession<BusTypes::Recv, BusTypes::Msgs>,
    Session::Config: for<'a> Deserialize<'a> + Serialize,
    Ctx: 'static + NSNameCachesCtx + Send
{
    type BusTypes = BusTypes;
    type Ctx = Ctx;
    type EpochsConfig = BusTypes::EpochsConfig;
    type IDsConfig = BusTypes::IDsConfig;
    type IDsCreateError = BusTypes::IDsCreateError;
    type LargeObjMsgAuthConfig = BusTypes::LargeObjMsgAuthConfig;
    type MsgAuthConfig = BusTypes::MsgAuthConfig;
    type MsgAuthCreateError = BusTypes::MsgAuthCreateError;
    type Msgs = BusTypes::Msgs;
    type Recv = BusTypes::Recv;
    type ResolverConfig = BusTypes::ResolverConfig;
    type Session = Session;
    type SessionAuthConfig = BusTypes::SessionAuthConfig;
    type SessionCleanup = Session::Cleanup;
    type SessionConfig = Session::Config;
    type SessionCreateError = Session::CreateError;
    type SessionParam = Session::Param;
    type SessionStartError = Session::StartError;
    type UDPConfig = BusTypes::UDPConfig;
    type UnixConfig = BusTypes::UnixConfig;
}

impl<Ctx, Param> UnicastClientParam<Ctx, Param> {
    #[inline]
    pub fn new(
        session_param: Param,
        ctx: Ctx
    ) -> Self {
        UnicastClientParam {
            session_param: session_param,
            ctx: ctx
        }
    }
}

impl<ClientTypes, Types> UnicastClientComponent<ClientTypes, Types>
where
    ClientTypes: 'static + ClientComponentTypes,
    Types: 'static + UnicastClientComponentTypes<ClientTypes>
{
    pub fn create(
        config: UnicastClientConfig<
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
        param: UnicastClientParam<Types::Ctx, Types::SessionParam>
    ) -> Result<(Self, Notify), Error> {
        let (config, session_config) = config.take();
        let UnicastClientParam { ctx, session_param } = param;
        let (starter, notify) = UnicastLargeObjBusStarter::create(config, ctx)?;

        Ok((
            UnicastClientComponent {
                starter: starter,
                session_param: session_param,
                session_config: session_config,
                notify: notify.clone()
            },
            notify
        ))
    }

    pub fn start(
        self,
        shutdown: ShutdownFlag
    ) -> Result<
        UnicastClientComponentCleanup<ClientTypes, Types>,
        UnicastClientComponentRunError<
            Types::SessionCreateError,
            UnicastLargeObjBusCreateError<
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
                >
            >,
            Types::SessionStartError
        >
    > {
        let UnicastClientComponent {
            starter,
            session_param,
            session_config,
            notify
        } = self;

        info!(target: "unicast-client-component",
              "starting unicast client component");

        let (session, recv, msgs) =
            Types::Session::create(session_config, notify, session_param)
                .map_err(|err| UnicastClientComponentRunError::Session {
                    err: err
                })?;
        let unicast = starter.start(recv, msgs, shutdown).map_err(|err| {
            UnicastClientComponentRunError::Unicast { err: err }
        })?;
        let session_cleanup = session.start().map_err(|err| {
            UnicastClientComponentRunError::Start { err: err }
        })?;

        Ok(UnicastClientComponentCleanup {
            unicast: unicast,
            session: session_cleanup
        })
    }
}

impl<ClientTypes, Types> HashUnicastClientComponent<ClientTypes, Types>
where
    ClientTypes: 'static + HashClientComponentTypes,
    Types: 'static + HashUnicastClientComponentTypes<ClientTypes>
{
    pub fn create(
        config: UnicastClientConfig<
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
        param: UnicastClientParam<Types::Ctx, Types::SessionParam>
    ) -> Result<(Self, Notify), Error> {
        let (config, session_config) = config.take();
        let UnicastClientParam { ctx, session_param } = param;
        let (starter, notify) = UnicastLargeObjBusStarter::create(config, ctx)?;

        Ok((
            HashUnicastClientComponent {
                starter: starter,
                session_param: session_param,
                session_config: session_config,
                notify: notify.clone()
            },
            notify
        ))
    }

    pub fn start(
        self,
        shutdown: ShutdownFlag
    ) -> Result<
        HashUnicastClientComponentCleanup<ClientTypes, Types>,
        UnicastClientComponentRunError<
            Types::SessionCreateError,
            UnicastLargeObjBusCreateError<
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
                >
            >,
            Types::SessionStartError
        >
    > {
        let HashUnicastClientComponent {
            starter,
            session_param,
            session_config,
            notify
        } = self;

        info!(target: "unicast-client-component",
              "starting unicast client component");

        let (session, recv, msgs) =
            Types::Session::create(session_config, notify, session_param)
                .map_err(|err| UnicastClientComponentRunError::Session {
                    err: err
                })?;
        let unicast = starter.start(recv, msgs, shutdown).map_err(|err| {
            UnicastClientComponentRunError::Unicast { err: err }
        })?;
        let session_cleanup = session.start().map_err(|err| {
            UnicastClientComponentRunError::Start { err: err }
        })?;

        Ok(HashUnicastClientComponentCleanup {
            unicast: unicast,
            session: session_cleanup
        })
    }
}

impl<ClientTypes, Types> UnicastClientComponentCleanup<ClientTypes, Types>
where
    ClientTypes: 'static + ClientComponentTypes,
    Types: 'static + UnicastClientComponentTypes<ClientTypes>
{
    pub fn cleanup(self) {
        let UnicastClientComponentCleanup { unicast, session } = self;

        unicast.cleanup();
        session.cleanup();
    }
}

impl<ClientTypes, Types> HashUnicastClientComponentCleanup<ClientTypes, Types>
where
    ClientTypes: 'static + HashClientComponentTypes,
    Types: 'static + HashUnicastClientComponentTypes<ClientTypes>
{
    pub fn cleanup(self) {
        let HashUnicastClientComponentCleanup { unicast, session } = self;

        unicast.cleanup();
        session.cleanup();
    }
}

impl<Session> Display for UnicastClientComponentCreateError<Session>
where
    Session: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            UnicastClientComponentCreateError::Session { err } => err.fmt(f),
            UnicastClientComponentCreateError::IO { err } => {
                write!(f, "{}", err)
            }
        }
    }
}

impl<Session, Unicast, Start> Display
    for UnicastClientComponentRunError<Session, Unicast, Start>
where
    Session: Display,
    Unicast: Display,
    Start: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            UnicastClientComponentRunError::Session { err } => err.fmt(f),
            UnicastClientComponentRunError::Unicast { err } => err.fmt(f),
            UnicastClientComponentRunError::Start { err } => err.fmt(f),
            UnicastClientComponentRunError::IO { err } => write!(f, "{}", err),
            UnicastClientComponentRunError::SkippedIdx => {
                write!(f, "stream parties skipped an index")
            }
        }
    }
}
