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
use std::net::SocketAddr;

use constellation_auth::cred::SSLCred;
use constellation_channels::far::compound::CompoundFarChannelSessionCred;
use constellation_channels::far::compound::CompoundFarChannelXfrmPeerAddr;
use constellation_channels::far::compound::CompoundFarIPChannelXfrmPeerAddr;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::config::CreateWithParam;
use constellation_common::shutdown::ShutdownFlag;
use constellation_component_common::config::UnicastLargeObjBusConfig;
use constellation_component_common::bus::unicast::UnicastBus;
use constellation_component_common::bus::unicast::UnicastBusCleanup;
use constellation_component_common::bus::unicast::UnicastBusCreateError;
use constellation_component_common::bus::unicast::UnicastBusTypes;
use constellation_streams::select::StreamSelectorCreateError;
use constellation_streams::threads::poll::PollThreadCreateError;
use constellation_streams::threads::poll::PollThreadTypes;
use log::debug;
use log::info;

use crate::config::UnicastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::ClientSessionTypes;
use crate::session::UnicastClientSession;

pub trait UnicastClientComponentTypes {
    type Addr: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type Ctx: 'static + NSNameCachesCtx + Send;
    type SessionPrin: Clone + Debug + Display + Eq + Hash;
    type ChansConfig: Default;
    type ChansCreateError: Debug + Display;
    type MsgAuthConfig;
    type MsgAuthCreateError: Debug + Display;
    type ThreadTypes: PollThreadTypes<
        Self::Ctx,
        Addr = Self::Addr,
        SessionPrin = Self::SessionPrin,
        ChansConfig = Self::ChansConfig,
        ChansCreateError = Self::ChansCreateError,
        MsgAuthConfig = Self::MsgAuthConfig,
        MsgAuthCreateError = Self::MsgAuthCreateError
    >;
    type EpochsConfig: Default;
    type EpochsCreateError: Debug + Display;
    type ModeCreateError: Debug + Display;
    type ResolveCreateError: Debug + Display;
    type BusTypes: UnicastBusTypes<
        Self::Ctx,
        ThreadTypes = Self::ThreadTypes,
        EpochsConfig = Self::EpochsConfig,
        EpochsCreateError = Self::EpochsCreateError,
        ResolveCreateError = Self::ResolveCreateError,
        ModeCreateError = Self::ModeCreateError,
        ChansCreateError = Self::ChansCreateError,
        MsgAuthCreateError = Self::MsgAuthCreateError
    > + Send;
    type SessionArgs;
    type SessionConfig;
    type SessionTypes: ClientSessionTypes<
        SessionPrin = Self::SessionPrin
    >;
    type SessionCleanup: ClientSessionCleanup;
    type SessionCreateError: Debug + Display;
    type SessionStartError: Debug + Display;
    type Session: UnicastClientSession<
        Self::SessionArgs,
        Self::SessionPrin,
        Cleanup = Self::SessionCleanup,
        StartError = Self::SessionStartError,
        Config = Self::SessionConfig,
        CreateError = Self::SessionCreateError
    >;
}

pub struct UnicastClientComponent<Types>
where
    Types: UnicastClientComponentTypes {
    config: UnicastLargeObjBusConfig<
        Types::ChansConfig,
        Types::EpochsConfig,
        Types::MsgAuthConfig,
        Types::Addr
    >,
    session_args: Types::SessionArgs,
    session_config: Types::SessionConfig,
    ctx: Types::Ctx
}

pub struct UnicastClientComponentCleanup<Session>
where
    Session: ClientSessionCleanup {
    shutdown: ShutdownFlag,
    unicast: UnicastBusCleanup,
    session: Session
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
    Types: UnicastClientComponentTypes {
    type Config = UnicastClientConfig<
        Types::SessionConfig,
        Types::ChansConfig,
        Types::EpochsConfig,
        Types::MsgAuthConfig,
        Types::Addr
    >;
    type CreateError = Infallible;

    fn create(
        config: UnicastClientConfig<
            Types::SessionConfig,
            Types::ChansConfig,
            Types::EpochsConfig,
            Types::MsgAuthConfig,
            Types::Addr
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
    Types: UnicastClientComponentTypes {
    pub fn start(
        self
    ) -> Result<
        UnicastClientComponentCleanup<Types::SessionCleanup>,
        UnicastClientComponentRunError<
            Types::SessionCreateError,
            UnicastBusCreateError<
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
        let UnicastClientComponent {
            config,
            session_args,
            session_config,
            ctx,
        } = self;

        info!(target: "unicast-client-component",
              "starting unicast client component");

        let unicast_config = config.take();
        let session = Types::Session::create(session_config, session_args)
            .map_err(|err| {
                UnicastClientComponentRunError::Session { err: err }
            })?;
        let unicast: UnicastBus<Types::BusTypes, Types::Ctx> =
            UnicastBus::create(
                unicast_config,
                ctx,
            )
            .map_err(|err| UnicastClientComponentRunError::Unicast {
                err: err
            })?;
        let session_cleanup = session.start().map_err(|err| {
            UnicastClientComponentRunError::Start { err: err }
        })?;

        debug!(target: "unicast-client-component",
               "starting unicaster");

        let unicast_cleanup = unicast
            .start()
            .map_err(|err| UnicastClientComponentRunError::IO { err: err })?;

        Ok(UnicastClientComponentCleanup {
            shutdown: shutdown,
            unicast: unicast_cleanup,
            session: session_cleanup
        })
    }
}

impl<Session> UnicastClientComponentCleanup<Session>
where
    Session: ClientSessionCleanup
{
    pub fn cleanup(self) {
        let UnicastClientComponentCleanup {
            mut shutdown,
            unicast,
            session
        } = self;

        shutdown.set();
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
