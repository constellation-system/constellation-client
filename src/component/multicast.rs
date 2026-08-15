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
use std::hash::Hash;
use std::io::Error;
use std::net::SocketAddr;

use constellation_auth::authn::AuthNed;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::cred::SSLCred;
use constellation_channels::far::compound::CompoundFarChannelSessionCred;
use constellation_channels::far::compound::CompoundFarChannelXfrmPeerAddr;
use constellation_channels::far::compound::CompoundFarIPChannelXfrmPeerAddr;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::shutdown::ShutdownFlag;
use constellation_component_common::config::MulticastLargeObjBusConfig;
use constellation_component_common::bus::multicast::MulticastBus;
use constellation_component_common::bus::multicast::MulticastBusCleanup;
use constellation_component_common::bus::multicast::MulticastBusCreateError;
use constellation_component_common::bus::multicast::MulticastBusTypes;
use constellation_streams::config::SharedLargeObjModeConfig;
use constellation_streams::large_obj::LargeObjProtoTypes;
use constellation_streams::select::StreamSelectorCreateError;
use constellation_streams::threads::poll::PollThreadCreateError;
use constellation_streams::threads::poll::PollThreadTypes;
use log::debug;
use log::info;

use crate::config::MulticastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::MulticastClientSession;

pub trait MulticastClientComponentTypes {
    type InMsg;
    type OutMsg;
    type AuthNMsg: AuthNed<Self::MsgPrin, Self::InMsg>;
    type Addr: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type MsgPrin: Clone + Display + Eq + Hash;
    type SessionPrin: Clone + Debug + Display + Eq + Hash;
    type ChansConfig: Default;
    type ChansCreateError: Debug + Display;
    type Msgs: 'static + Send;
    type Recv: 'static
        + AuthNMsgRecv<
            Self::MsgPrin,
            Self::InMsg,
            Self::AuthNMsg,
        >
        + Send;
    type MsgAuthConfig;
    type MsgAuthCreateError: Debug + Display;
    type ThreadTypes: PollThreadTypes<
        Self::Ctx,
        Addr = Self::Addr,
        SessionPrin = Self::SessionPrin,
        Msgs = Self::Msgs,
        Recv = Self::Recv,
        ChansConfig = Self::ChansConfig,
        ChansCreateError = Self::ChansCreateError,
        MsgAuthConfig = Self::MsgAuthConfig,
        MsgAuthCreateError = Self::MsgAuthCreateError
    >;
    type EpochsConfig: Default;
    type EpochsCreateError: Debug + Display;
    type ModeCreateError: Debug + Display;
    type ResolveCreateError: Debug + Display;
    type BusTypes: MulticastBusTypes<
        Self::Ctx,
        Addr = Self::Addr,
        SessionPrin = Self::SessionPrin,
        Msgs = Self::Msgs,
        Recv = Self::Recv,
        ThreadTypes = Self::ThreadTypes,
        EpochsConfig = Self::EpochsConfig,
        EpochsCreateError = Self::EpochsCreateError,
        ResolveCreateError = Self::ResolveCreateError,
        ModeConfig = SharedLargeObjModeConfig,
        ModeCreateError = Self::ModeCreateError,
        ChansConfig = Self::ChansConfig,
        ChansCreateError = Self::ChansCreateError,
        MsgAuthConfig = Self::MsgAuthConfig,
        MsgAuthCreateError = Self::MsgAuthCreateError
    > + Send;
    type SessionArgs;
    type SessionConfig;
    type SessionTypes: LargeObjProtoTypes<
        Self::InMsg, Self::OutMsg,
        SessionPrin = Self::SessionPrin,
        Msgs = Self::Msgs,
        Recv = Self::Recv,
    >;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: MulticastClientSession<
        Self::SessionTypes,
        Self::InMsg,
        Self::OutMsg,
        Self::SessionArgs,
        Cleanup = Self::SessionCleanup,
        StartError = Self::SessionStartError,
        Config = Self::SessionConfig,
        CreateError = Self::SessionCreateError
    >;
}

pub struct MulticastClientComponent<Types>
where
    Types: MulticastClientComponentTypes {
    config: MulticastLargeObjBusConfig<
        Types::ChansConfig,
        Types::EpochsConfig,
        Types::SessionPrin,
        Types::MsgAuthConfig,
        Types::Addr
    >,
    self_party: Types::SessionPrin,
    session_args: Types::SessionArgs,
    session_config: Types::SessionConfig,
    ctx: Types::Ctx
}

pub struct MulticastClientComponentCleanup<Session>
where
    Session: ClientSessionCleanup {
    shutdown: ShutdownFlag,
    multicast: MulticastBusCleanup,
    session: Session
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

impl<Types> MulticastClientComponent<Types>
where
    Types: MulticastClientComponentTypes {
    pub fn create(
        config: MulticastClientConfig<
            Types::SessionConfig,
            Types::ChansConfig,
            Types::EpochsConfig,
            Types::SessionPrin,
            Types::MsgAuthConfig,
            Types::Addr
        >,
        param: MulticastClientParam<Types::Ctx, Types::SessionArgs>
    ) -> Self {
        let (self_party, config, session_config) = config.take();
        let MulticastClientParam { ctx, session_args } = param;

        MulticastClientComponent {
            config: config,
            self_party: self_party,
            session_args: session_args,
            session_config: session_config,
            ctx: ctx
        }
    }
}

impl<Types> MulticastClientComponent<Types>
where
    Types: MulticastClientComponentTypes {
    pub fn start(
        self
    ) -> Result<
        MulticastClientComponentCleanup<Types::SessionCleanup>,
        MulticastClientComponentRunError<
            Types::SessionCreateError,
            MulticastBusCreateError<
                PollThreadCreateError<
                    Types::ModeCreateError,
                    Types::ChansCreateError,
                    StreamSelectorCreateError<
                        Types::ResolveCreateError,
                        Types::EpochsCreateError
                    >,
                    Types::MsgAuthCreateError
                >
            >,
            Types::SessionStartError,
        >
    >{
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
            Types::Session::create(session_config, session_args)
            .map_err(|err| {
                MulticastClientComponentRunError::Session { err: err }
            })?;
        let multicast: MulticastBus<Types::BusTypes, Types::Ctx> =
            MulticastBus::create(
                config,
                Some(self.self_party),
                ctx,
                recv,
                msgs
            )
            .map_err(|err| {
                MulticastClientComponentRunError::Multicast { err: err }
            })?;
        let Ok(parties) = multicast.parties();
        let session_cleanup = session.start(parties).map_err(|err| {
            MulticastClientComponentRunError::Start { err: err }
        })?;

        debug!(target: "multicast-client-component",
               "starting multicaster");

        let multicast_cleanup = multicast
            .start()
            .map_err(|err| MulticastClientComponentRunError::IO { err: err })?;

        Ok(MulticastClientComponentCleanup {
            shutdown: shutdown,
            multicast: multicast_cleanup,
            session: session_cleanup
        })
    }
}

impl<Session> MulticastClientComponentCleanup<Session>
where
    Session: ClientSessionCleanup
{
    pub fn cleanup(self) {
        let MulticastClientComponentCleanup {
            mut shutdown,
            multicast,
            session
        } = self;

        //        shutdown.set();
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
/*
// ISSUE #2: Delete from here

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum TestCred {
    IP { addr: SocketAddr },
    Unix { addr: UnixSocketAddr }
}

impl<Basic> From<SSLCred<CompoundFarChannelSessionCred<Basic>>> for TestCred
where
    TestCred: From<Basic>
{
    fn from(_val: SSLCred<CompoundFarChannelSessionCred<Basic>>) -> TestCred {
        panic!("Not supported!")
    }
}

impl From<CompoundFarIPChannelXfrmPeerAddr> for TestCred {
    fn from(val: CompoundFarIPChannelXfrmPeerAddr) -> TestCred {
        match val {
            CompoundFarIPChannelXfrmPeerAddr::UDP { udp } => {
                TestCred::IP { addr: udp }
            }
            _ => panic!("Not supported!")
        }
    }
}

impl From<CompoundFarChannelXfrmPeerAddr> for TestCred {
    fn from(val: CompoundFarChannelXfrmPeerAddr) -> TestCred {
        match val {
            CompoundFarChannelXfrmPeerAddr::Unix { unix } => {
                TestCred::Unix { addr: unix }
            }
            CompoundFarChannelXfrmPeerAddr::IP { ip } => TestCred::from(ip)
        }
    }
}

impl<Basic> From<CompoundFarChannelSessionCred<Basic>> for TestCred
where
    TestCred: From<Basic>
{
    fn from(val: CompoundFarChannelSessionCred<Basic>) -> TestCred {
        match val {
            CompoundFarChannelSessionCred::Basic { basic } => {
                TestCred::from(basic)
            }
            _ => panic!("Not supported!")
        }
    }
}

impl Display for TestCred {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            TestCred::IP { addr } => write!(f, "ip://{}", addr),
            TestCred::Unix { addr } => write!(f, "unix://{}", addr)
        }
    }
}

// ISSUE #2: to here
*/
