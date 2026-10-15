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

use crate::component::ClientComponentTypes;
use crate::config::UnicastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::UnicastClientSession;

pub trait UnicastClientComponentTypes: ClientComponentTypes {
    type IDsConfig: Default;
    type IDsCreateError: Debug + Display;
    type ResolverConfig: Default;
    type EpochsConfig: Default;
    type SessionAuthConfig;
    type MsgAuthConfig;
    type LargeObjMsgAuthConfig;
    type UnixConfig: Default;
    type UDPConfig: Default;
    type MsgAuthCreateError: Debug + Display;
    type Msgs: 'static + Send;
    type Recv: 'static + AuthNMsgRecv<Self::MsgPrin, Self::AuthNMsg> + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionArgs;
    type SessionConfig;
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
                Self::RoundID,
                Self::HashID,
                Self::Seal,
                Self::Payload,
                Self::Effect,
                Self::Result,
                Self::Error
            >,
            MsgPrin = Self::MsgPrin,
            AuthNMsg = Self::AuthNMsg,
            Msgs = Self::Msgs,
            Recv = Self::Recv,
            EpochsConfig = Self::EpochsConfig,
            UnixConfig = Self::UnixConfig,
            UDPConfig = Self::UDPConfig,
            HashConfig = Self::HashConfig,
            HashCreateError = Self::HashCreateError,
            ResolverConfig = Self::ResolverConfig,
            SessionAuthConfig = Self::SessionAuthConfig,
            LargeObjMsgAuthConfig = Self::LargeObjMsgAuthConfig,
            MsgAuthConfig = Self::MsgAuthConfig,
            MsgAuthCreateError = Self::MsgAuthCreateError,
            DecoderConfig = XactBatchCodecConfig<
                Self::SealCodecConfig,
                Self::PayloadCodecConfig,
                Self::EffectCodecConfig,
                Self::ResultCodecConfig,
                Self::ErrorCodecConfig
            >,
            DecoderCreateError = XactBatchCodecCreateError<
                XactSealedCodecCreateError<
                    Self::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Self::PayloadCodecCreateError,
                        Self::EffectCodecCreateError
                    >
                >,
                XactCommittedRoundCodecCreateError<
                    Self::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Self::PayloadCodecCreateError,
                        Self::EffectCodecCreateError
                    >
                >,
                XactNotifyCodecCreateError<
                    Self::ResultCodecCreateError,
                    Self::ErrorCodecCreateError
                >
            >,
            EncoderConfig = XactBatchCodecConfig<
                Self::SealCodecConfig,
                Self::PayloadCodecConfig,
                Self::EffectCodecConfig,
                Self::ResultCodecConfig,
                Self::ErrorCodecConfig
            >,
            EncoderCreateError = XactBatchCodecCreateError<
                XactSealedCodecCreateError<
                    Self::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Self::PayloadCodecCreateError,
                        Self::EffectCodecCreateError
                    >
                >,
                XactCommittedRoundCodecCreateError<
                    Self::SealCodecCreateError,
                    XactReqCodecCreateError<
                        Self::PayloadCodecCreateError,
                        Self::EffectCodecCreateError
                    >
                >,
                XactNotifyCodecCreateError<
                    Self::ResultCodecCreateError,
                    Self::ErrorCodecCreateError
                >
            >,
            IDsConfig = Self::IDsConfig,
            IDsCreateError = Self::IDsCreateError
        > + Send;
}

pub struct UnicastClientComponent<Types>
where
    Types: UnicastClientComponentTypes {
    config: UnicastLargeObjBusConfig<
        Types::HashConfig,
        Types::SessionAuthConfig,
        Types::LargeObjMsgAuthConfig,
        Types::UnixConfig,
        Types::UDPConfig,
        XactBatchCodecConfig<
            Types::SealCodecConfig,
            Types::PayloadCodecConfig,
            Types::EffectCodecConfig,
            Types::ResultCodecConfig,
            Types::ErrorCodecConfig
        >,
        XactBatchCodecConfig<
            Types::SealCodecConfig,
            Types::PayloadCodecConfig,
            Types::EffectCodecConfig,
            Types::ResultCodecConfig,
            Types::ErrorCodecConfig
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

pub struct UnicastClientComponentCleanup<Types>
where
    Types: UnicastClientComponentTypes {
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

impl<Types> CreateWithParam<UnicastClientParam<Types::Ctx, Types::SessionArgs>>
    for UnicastClientComponent<Types>
where
    Types: UnicastClientComponentTypes
{
    type Config = UnicastClientConfig<
        Types::SessionConfig,
        Types::SealCodecConfig,
        Types::PayloadCodecConfig,
        Types::EffectCodecConfig,
        Types::ResultCodecConfig,
        Types::ErrorCodecConfig,
        Types::HashConfig,
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
            Types::SealCodecConfig,
            Types::PayloadCodecConfig,
            Types::EffectCodecConfig,
            Types::ResultCodecConfig,
            Types::ErrorCodecConfig,
            Types::HashConfig,
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

impl<Types> UnicastClientComponent<Types>
where
    Types: 'static + UnicastClientComponentTypes
{
    pub fn start(
        self
    ) -> Result<
        UnicastClientComponentCleanup<Types>,
        UnicastClientComponentRunError<
            Types::SessionCreateError,
            UnicastLargeObjBusCreateError<
                Types::HashCreateError,
                Types::MsgAuthCreateError,
                LargeObjProtoCreateError<
                    XactBatchCodecCreateError<
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
                    XactBatchCodecCreateError<
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

impl<Types> UnicastClientComponentCleanup<Types>
where
    Types: 'static + UnicastClientComponentTypes
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
