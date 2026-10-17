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
use std::io::Error;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::config::CreateWithParam;
use constellation_component_common::bus::types::UnicastLargeObjBusTypes;
use constellation_component_common::bus::unicast::UnicastLargeObjBus;
use constellation_component_common::bus::unicast::UnicastLargeObjBusCreateError;
use constellation_component_common::config::UnicastLargeObjBusConfig;
use constellation_component_common::config::XactBatchCodecConfig;
use constellation_component_common::xact::XactHashBatch;
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
use crate::config::UnicastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::UnicastClientSession;

pub trait UnicastClientComponentTypes<Types>
where Types: ClientComponentTypes {
    type IDsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type IDsCreateError: Debug + Display;
    type ResolverConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type EpochsConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type SessionAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type MsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type LargeObjMsgAuthConfig: for<'a> Deserialize<'a> + Serialize;
    type UnixConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type UDPConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type MsgAuthCreateError: Debug + Display;
    type Msgs: 'static + Send;
    type Recv: 'static + AuthNMsgRecv<Types::MsgPrin, Types::AuthNMsg> + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionArgs;
    type SessionConfig: for<'a> Deserialize<'a> + Serialize;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: UnicastClientSession<
            Self::Recv,
            Self::Msgs,
            Self::SessionArgs,
            Cleanup = Self::SessionCleanup,
            Config = Self::SessionConfig,
            CreateError = Self::SessionCreateError,
            StartError = Self::SessionStartError
        >;
    type BusTypes: UnicastLargeObjBusTypes<
            Self::Ctx,
            InMsg = XactHashBatch<
                Types::RoundID,
                Types::HashID,
                Types::Seal,
                Types::Payload,
                Types::Effect,
                Types::Result,
                Types::Error
            >,
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

pub struct UnicastClientComponent<ClientTypes, Types>
where
    ClientTypes: ClientComponentTypes,
    Types: UnicastClientComponentTypes<ClientTypes> {
    config: UnicastLargeObjBusConfig<
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
    session_args: Types::SessionArgs,
    session_config: Types::SessionConfig,
    ctx: Types::Ctx
}

pub struct UnicastClientComponentCleanup<ClientTypes, Types>
where
    ClientTypes: ClientComponentTypes,
    Types: UnicastClientComponentTypes<ClientTypes> {
    unicast: UnicastLargeObjBus<Types::BusTypes, Types::Ctx>,
    session: Types::SessionCleanup
}

pub struct UnicastClientParam<Ctx, Args> {
    session_args: Args,
    ctx: Ctx
}

#[derive(Debug)]
pub enum UnicastClientComponentRunError<Session, Unicast, Start> {
    Session { err: Session },
    Unicast { err: Unicast },
    Start { err: Start },
    IO { err: Error },
    SkippedIdx
}

impl<Ctx, Args> UnicastClientParam<Ctx, Args> {
    #[inline]
    pub fn new(
        session_args: Args,
        ctx: Ctx
    ) -> Self {
        UnicastClientParam {
            session_args: session_args,
            ctx: ctx
        }
    }
}

impl<ClientTypes, Types>
    CreateWithParam<UnicastClientParam<Types::Ctx, Types::SessionArgs>>
    for UnicastClientComponent<ClientTypes, Types>
where
    ClientTypes: ClientComponentTypes,
    Types: UnicastClientComponentTypes<ClientTypes>
{
    type Config = UnicastClientConfig<
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
        param: UnicastClientParam<Types::Ctx, Types::SessionArgs>
    ) -> Result<Self, Self::CreateError> {
        let (config, session_config) = config.take();
        let UnicastClientParam { ctx, session_args } = param;

        Ok(UnicastClientComponent {
            config: config,
            session_args: session_args,
            session_config: session_config,
            ctx: ctx
        })
    }
}

impl<ClientTypes, Types> UnicastClientComponent<ClientTypes, Types>
where
    ClientTypes: 'static + ClientComponentTypes,
    Types: 'static + UnicastClientComponentTypes<ClientTypes>
{
    pub fn start(
        self
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
            config,
            session_args,
            session_config,
            ctx
        } = self;

        info!(target: "unicast-client-component",
              "starting unicast client component");

        let (session, recv, msgs) =
            Types::Session::create(session_config, session_args).map_err(
                |err| UnicastClientComponentRunError::Session { err: err }
            )?;
        let unicast = UnicastLargeObjBus::start(config, ctx, recv, msgs)
            .map_err(|err| UnicastClientComponentRunError::Unicast {
                err: err
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
