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

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNed;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::MutexPoison;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
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

use crate::config::MulticastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::MulticastClientSession;

pub trait MulticastClientComponentTypes {
    type RoundID: Clone + From<u128> + Into<u128>;
    type Seal;
    type SealCodecConfig: Default;
    type SealCodecCreateError: Debug + Display;
    type Payload;
    type PayloadCodecConfig: Default;
    type PayloadCodecCreateError: Debug + Display;
    type Effect;
    type EffectCodecConfig: Default;
    type EffectCodecCreateError: Debug + Display;
    type Result;
    type ResultCodecConfig: Default;
    type ResultCodecCreateError: Debug + Display;
    type Error;
    type ErrorCodecConfig: Default;
    type ErrorCodecCreateError: Debug + Display;
    type HashID: Clone + Display + Eq + Hash + HashID;
    type Hash: Default
        + HashAlgo<HashID = Self::HashID>
        + Create<Config = Self::HashConfig, CreateError = Self::HashCreateError>;
    type HashConfig: Default;
    type HashCreateError: Debug + Display;
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
    type AuthNMsg: AuthNed<Self::MsgPrin>;
    type MsgPrin: Clone + Display + Eq + Hash;
    type Msgs: 'static + Send;
    type Recv: 'static + AuthNMsgRecv<Self::MsgPrin, Self::AuthNMsg> + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionArgs;
    type SessionConfig;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: MulticastClientSession<
            String,
            Self::Recv,
            Self::Msgs,
            Self::SessionArgs,
            Cleanup = Self::SessionCleanup,
            Config = Self::SessionConfig,
            CreateError = Self::SessionCreateError,
            StartError = Self::SessionStartError
        >;
    type BusTypes: MulticastLargeObjBusTypes<
            Self::Ctx,
            SessionPrin = String,
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

pub struct MulticastClientComponent<Types>
where
    Types: MulticastClientComponentTypes {
    config: MulticastLargeObjBusConfig<
        String,
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

pub struct MulticastClientComponentCleanup<Types>
where
    Types: MulticastClientComponentTypes {
    multicast: MulticastLargeObjBus<Types::BusTypes, Types::Ctx>,
    session: Types::SessionCleanup
}

pub struct MulticastClientParam<Ctx, Args> {
    session_args: Args,
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

impl<Ctx, Args> MulticastClientParam<Ctx, Args> {
    #[inline]
    pub fn new(
        session_args: Args,
        ctx: Ctx
    ) -> Self {
        MulticastClientParam {
            session_args: session_args,
            ctx: ctx
        }
    }
}

impl<Types>
    CreateWithParam<MulticastClientParam<Types::Ctx, Types::SessionArgs>>
    for MulticastClientComponent<Types>
where
    Types: MulticastClientComponentTypes
{
    type Config = MulticastClientConfig<
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
        config: MulticastClientConfig<
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
        param: MulticastClientParam<Types::Ctx, Types::SessionArgs>
    ) -> Result<Self, Self::CreateError> {
        let (config, session_config) = config.take();
        let MulticastClientParam { ctx, session_args } = param;

        Ok(MulticastClientComponent {
            config: config,
            session_args: session_args,
            session_config: session_config,
            ctx: ctx
        })
    }
}

impl<Types> MulticastClientComponent<Types>
where
    Types: 'static + MulticastClientComponentTypes
{
    pub fn start(
        self
    ) -> Result<
        MulticastClientComponentCleanup<Types>,
        MulticastClientComponentRunError<
            Types::SessionCreateError,
            MulticastLargeObjBusCreateError<
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
                >,
                MutexPoison
            >,
            Types::SessionStartError
        >
    > {
        let MulticastClientComponent {
            session_args,
            session_config,
            config,
            ctx,
            ..
        } = self;

        info!(target: "multicast-client-component",
              "starting multicast client component");

        let (session, recv, msgs) =
            Types::Session::create(session_config, session_args).map_err(
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

impl<Types> MulticastClientComponentCleanup<Types>
where
    Types: 'static + MulticastClientComponentTypes
{
    pub fn cleanup(self) {
        let MulticastClientComponentCleanup { multicast, session } = self;

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
