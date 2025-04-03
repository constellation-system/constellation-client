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

use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::authn::SessionAuthN;
use constellation_auth::authn::TrivialAuthN;
use constellation_auth::cred::SSLCred;
use constellation_channels::config::ChannelRegistryChannelsConfig;
use constellation_channels::config::CompoundFarEndpoint;
use constellation_channels::config::ResolverConfig;
use constellation_channels::far::compound::CompoundFarChannel;
use constellation_channels::far::compound::CompoundFarChannelSessionCred;
use constellation_channels::far::compound::CompoundFarChannelThreadedFlows;
use constellation_channels::far::compound::CompoundFarChannelXfrm;
use constellation_channels::far::compound::CompoundFarChannelXfrmPeerAddr;
use constellation_channels::far::compound::CompoundFarIPChannelXfrmPeerAddr;
use constellation_channels::far::flows::OwnedFlowNegotiator;
use constellation_channels::far::flows::OwnedFlowsCreate;
use constellation_channels::far::flows::ThreadedFlowsListener;
use constellation_channels::far::registry::FarChannelRegistryAcquireError;
use constellation_channels::far::registry::FarChannelRegistryChannelsCreateError;
use constellation_channels::far::registry::FarChannelRegistryCtx;
use constellation_channels::far::registry::FarChannelRegistryID;
use constellation_channels::far::registry::RegistryAcquireError;
use constellation_channels::far::udp::UDPDatagramXfrm;
use constellation_channels::far::unix::UnixDatagramXfrm;
use constellation_channels::far::FarChannelAcquired;
use constellation_channels::far::FarChannelAcquiredResolve;
use constellation_channels::far::FarChannelCreate;
use constellation_channels::far::FarChannelFlowsError;
use constellation_channels::far::FarChannelOwnedFlows;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::MixedResolver;
use constellation_channels::unix::UnixSocketAddr;
use constellation_common::codec::Codec;
use constellation_common::hashid::HashAlgo;
use constellation_common::ids::IDGen;
use constellation_common::net::DatagramXfrm;
use constellation_common::net::DatagramXfrmCreate;
use constellation_common::net::IPEndpointAddr;
use constellation_common::net::Socket;
use constellation_common::shutdown::ShutdownFlag;
use constellation_component_common::bus::large_obj::unicast::UnicastLargeObjBus;
use constellation_component_common::bus::large_obj::unicast::UnicastLargeObjBusCleanup;
use constellation_component_common::bus::large_obj::unicast::UnicastLargeObjBusCreateError;
use constellation_streams::addrs::Addrs;
use constellation_streams::addrs::AddrsCreate;
use constellation_streams::channels::ChannelParam;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjMsgCodec;
use constellation_streams::select::StreamSelectorCreateError;
use constellation_streams::select::ThreadedStreamSelectorError;
use constellation_streams::stream::ConcurrentStream;
use constellation_streams::stream::StreamID;
use log::debug;
use log::info;

use crate::config::UnicastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::UnicastClientSession;

pub type CompoundUnicastClientComponent<
    Msg,
    Wrapper,
    MsgAuth,
    WrapperCodec,
    H,
    IDs,
    Recv,
    Session,
    Epochs,
    Ctx
> = UnicastClientComponent<
    Msg,
    Wrapper,
    MsgAuth,
    WrapperCodec,
    H,
    IDs,
    Recv,
    Session,
    Epochs,
    CompoundFarChannel,
    CompoundFarChannelThreadedFlows<
        TrivialAuthN<TestCred>,
        UnixDatagramXfrm,
        UDPDatagramXfrm,
        FarChannelRegistryID
    >,
    TrivialAuthN<TestCred>,
    CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>,
    Ctx,
    MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>,
    CompoundFarEndpoint
>;

pub struct UnicastClientComponent<
    Msg,
    Wrapper,
    MsgAuth,
    WrapperCodec,
    H,
    IDs,
    Recv,
    Session,
    Epochs,
    Channel,
    F,
    SessionAuth,
    Xfrm,
    Ctx,
    Resolver,
    Endpoint
> where
    Recv: Clone + AuthNMsgRecv<MsgAuth::Prin, Msg> + Send,
    IDs: Clone + IDGen + Iterator<Item = LargeObjID> + Send,
    MsgAuth:
        Clone + MsgAuthN<Msg, Wrapper, SessionPrin = SessionAuth::Prin> + Send,
    MsgAuth::SessionPrin: Send + Sync,
    Msg: Clone + Send,
    Wrapper: Clone + Send,
    WrapperCodec: Clone + Codec<Wrapper> + Send,
    <WrapperCodec as Codec<Wrapper>>::Param: Default,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + Eq + Send,
    Session: UnicastClientSession<
        H::HashID,
        Msg,
        Wrapper,
        MsgAuth,
        WrapperCodec,
        IDs,
        Recv
    >,
    Epochs: 'static + IDGen + Iterator + Send + Sync,
    Epochs::Item: Clone + Default + Display + Ord + Send,
    SessionAuth: Clone
        + SessionAuthN<<Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow>
        + Send
        + Sync,
    SessionAuth::Prin: 'static + Clone + Display + Eq + Hash + Send + Sync,
    Channel: FarChannelOwnedFlows<F, SessionAuth, Xfrm>
        + FarChannelCreate
        + Send
        + Sync,
    Channel::Acquired: FarChannelAcquiredResolve<Resolved = Channel::Param>,
    Channel::Param: 'static
        + Clone
        + Display
        + Eq
        + Hash
        + PartialEq
        + ChannelParam<<Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + Send
        + Sync,
    Channel::Acquired:
        FarChannelAcquiredResolve<Resolved = Channel::Param> + Send + Sync,
    <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow:
        ConcurrentStream + Send,
    <Channel::Xfrm as DatagramXfrm>::PeerAddr: Eq + Hash + Send + Sync,
    F: OwnedFlowsCreate<
            Channel::Socket,
            Channel::Nego,
            SessionAuth,
            Channel::Xfrm
        > + Send,
    F::Flow: 'static + ConcurrentStream + Send,
    F::CreateParam: Clone + Default + Send + Sync,
    F::Reporter: Clone + Send + Sync,
    F::ChannelID: From<usize> + Into<usize> + Send + Sync,
    Xfrm:
        DatagramXfrm + DatagramXfrmCreate<Addr = Channel::Param> + Send + Sync,
    Xfrm::CreateParam: Clone + Default + Send + Sync,
    Xfrm::LocalAddr: From<<Channel::Socket as Socket>::Addr>,
    Ctx: 'static
        + FarChannelRegistryCtx<Channel, F, SessionAuth, Xfrm>
        + NSNameCachesCtx
        + Send
        + Sync,
    Ctx::NameCaches: NSNameCachesCtx,
    Endpoint: 'static + Send,
    Resolver: 'static
        + Addrs<Addr = <Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + AddrsCreate<Ctx, Vec<Endpoint>, Config = ResolverConfig>
        + Send
        + Sync,
    Resolver::Origin: 'static
        + Clone
        + Eq
        + Hash
        + Into<Option<IPEndpointAddr>>
        + Send
        + Sync {
    channel: PhantomData<Channel>,
    flow: PhantomData<F>,
    xfrm: PhantomData<Xfrm>,
    resolver: PhantomData<Resolver>,
    session: PhantomData<Session>,
    config: UnicastClientConfig<
        ChannelRegistryChannelsConfig<
            <LargeObjMsgCodec<H> as Codec<LargeObjMsg<H::HashID>>>::Param
        >,
        Epochs::Config,
        Endpoint
    >,
    session_config: Session::Config,
    listener: ThreadedFlowsListener<
        <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow,
        StreamID<
            <Channel::Xfrm as DatagramXfrm>::PeerAddr,
            F::ChannelID,
            Channel::Param
        >,
        SessionAuth::Prin
    >,
    shutdown: ShutdownFlag,
    ctx: Ctx
}

pub struct UnicastClientComponentCleanup<Session>
where
    Session: ClientSessionCleanup {
    shutdown: ShutdownFlag,
    unicast: UnicastLargeObjBusCleanup,
    session: Session
}

#[derive(Debug)]
pub enum UnicastClientComponentRunError<Session, Unicast, Start> {
    Session { err: Session },
    Unicast { err: Unicast },
    Start { err: Start },
    SkippedIdx
}

impl<
        Msg,
        Wrapper,
        MsgAuth,
        WrapperCodec,
        H,
        IDs,
        Recv,
        Session,
        Epochs,
        Channel,
        F,
        SessionAuth,
        Xfrm,
        Ctx,
        Resolver,
        Endpoint
    >
    UnicastClientComponent<
        Msg,
        Wrapper,
        MsgAuth,
        WrapperCodec,
        H,
        IDs,
        Recv,
        Session,
        Epochs,
        Channel,
        F,
        SessionAuth,
        Xfrm,
        Ctx,
        Resolver,
        Endpoint
    >
where
    Recv: 'static + Clone + AuthNMsgRecv<MsgAuth::Prin, Msg> + Send,
    IDs: 'static + Clone + IDGen + Iterator<Item = LargeObjID> + Send,
    Msg: 'static + Clone + Send,
    Wrapper: 'static + Clone + Send,
    WrapperCodec: 'static + Clone + Codec<Wrapper> + Send,
    MsgAuth: 'static
        + Clone
        + MsgAuthN<Msg, Wrapper, SessionPrin = SessionAuth::Prin>
        + Send,
    MsgAuth::SessionPrin: Send + Sync,
    H: 'static + Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + Eq + Send,
    Session: UnicastClientSession<
        H::HashID,
        Msg,
        Wrapper,
        MsgAuth,
        WrapperCodec,
        IDs,
        Recv
    >,
    Epochs: 'static + IDGen + Iterator<Item = u128> + Send + Sync,
    Epochs::Item: Clone + Default + Display + Ord + Send,
    SessionAuth: 'static
        + Clone
        + SessionAuthN<<Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow>
        + Send
        + Sync,
    SessionAuth::Prin: 'static + Clone + Display + Eq + Hash + Send + Sync,
    <WrapperCodec as Codec<Wrapper>>::Param: Default,
    Channel: 'static
        + FarChannelOwnedFlows<F, SessionAuth, Xfrm>
        + FarChannelCreate
        + Send
        + Sync,
    Channel::Acquired: FarChannelAcquiredResolve<Resolved = Channel::Param>,
    Channel::Param: 'static
        + Clone
        + Display
        + Eq
        + Hash
        + PartialEq
        + ChannelParam<<Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + Send
        + Sync,
    Channel::Acquired:
        FarChannelAcquiredResolve<Resolved = Channel::Param> + Send + Sync,
    <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow:
        ConcurrentStream + Send,
    <Channel::Xfrm as DatagramXfrm>::PeerAddr: Eq + Hash + Send + Sync,
    F: 'static
        + OwnedFlowsCreate<
            Channel::Socket,
            Channel::Nego,
            SessionAuth,
            Channel::Xfrm
        >
        + Send,
    F::Flow: 'static + ConcurrentStream + Send,
    F::CreateParam: Clone + Default + Send + Sync,
    F::Reporter: Clone + Send + Sync,
    F::ChannelID: From<usize> + Into<usize> + Send + Sync,
    Xfrm: 'static
        + DatagramXfrm
        + DatagramXfrmCreate<Addr = Channel::Param>
        + Send
        + Sync,
    Xfrm::CreateParam: Clone + Default + Send + Sync,
    Xfrm::LocalAddr: From<<Channel::Socket as Socket>::Addr>,
    Ctx: 'static
        + FarChannelRegistryCtx<Channel, F, SessionAuth, Xfrm>
        + NSNameCachesCtx
        + Send
        + Sync,
    Ctx::NameCaches: NSNameCachesCtx,
    Endpoint: 'static + Send,
    Resolver: 'static
        + Addrs<Addr = <Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + AddrsCreate<Ctx, Vec<Endpoint>, Config = ResolverConfig>
        + Send
        + Sync,
    Resolver::Origin: 'static
        + Clone
        + Eq
        + Hash
        + Into<Option<IPEndpointAddr>>
        + Send
        + Sync
{
    pub fn create(
        config: UnicastClientConfig<
            ChannelRegistryChannelsConfig<()>,
            Epochs::Config,
            Endpoint
        >,
        session_config: Session::Config,
        listener: ThreadedFlowsListener<
            <Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow,
            StreamID<
                <Channel::Xfrm as DatagramXfrm>::PeerAddr,
                F::ChannelID,
                Channel::Param
            >,
            SessionAuth::Prin
        >,
        shutdown: ShutdownFlag,
        ctx: Ctx
    ) -> Self {
        UnicastClientComponent {
            channel: PhantomData,
            flow: PhantomData,
            xfrm: PhantomData,
            resolver: PhantomData,
            session: PhantomData,
            config: config,
            session_config: session_config,
            listener: listener,
            shutdown: shutdown,
            ctx: ctx
        }
    }

    pub fn start(
        self
    ) -> Result<
        UnicastClientComponentCleanup<Session::Cleanup>,
        UnicastClientComponentRunError<
            Session::CreateError,
            UnicastLargeObjBusCreateError<
                FarChannelRegistryAcquireError<
                    RegistryAcquireError<
                        Channel::AcquireError,
                        <Channel::Acquired as FarChannelAcquiredResolve>::ResolverError,
                        FarChannelFlowsError<
                            Channel::SocketError,
                            F::CreateError,
                            Channel::XfrmError
                        >,
                        <Channel::Acquired as FarChannelAcquired>::WrapError
                    >
                >,
                Infallible,
                StreamSelectorCreateError<
                    FarChannelRegistryChannelsCreateError<Infallible>,
                    Resolver::CreateError
                >,
                ThreadedStreamSelectorError<
                    Resolver::AddrsError,
                    FarChannelRegistryAcquireError<
                        RegistryAcquireError<
                            Channel::AcquireError,
                            <Channel::Acquired as FarChannelAcquiredResolve>::ResolverError,
                            FarChannelFlowsError<
                                Channel::SocketError,
                                F::CreateError,
                                Channel::XfrmError
                            >,
                            <Channel::Acquired as FarChannelAcquired>::WrapError
                        >
                    >
                >
            >,
            Session::StartError,
        >
    >{
        let UnicastClientComponent {
            config,
            session_config,
            listener,
            ctx,
            shutdown,
            ..
        } = self;

        info!(target: "unicast-client-component",
              "starting unicast client component");

        let unicast_config = config.take();
        let (session, notify, proto) = Session::create(session_config)
            .map_err(|err| UnicastClientComponentRunError::Session {
                err: err
            })?;
        let unicast: UnicastLargeObjBus<
            _,
            _,
            WrapperCodec,
            H,
            _,
            _,
            _,
            Epochs,
            _,
            _,
            _,
            _,
            Resolver,
            _,
            _
        > = UnicastLargeObjBus::create(
            unicast_config,
            listener,
            ctx,
            shutdown.clone(),
            notify,
            proto.clone()
        )
        .map_err(|err| UnicastClientComponentRunError::Unicast { err: err })?;
        let session_cleanup = session.start().map_err(|err| {
            UnicastClientComponentRunError::Start { err: err }
        })?;

        debug!(target: "unicast-client-component",
               "starting unicaster");

        let unicast_cleanup = unicast.start();

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
    ) -> Result<(), Error> {
        match self {
            UnicastClientComponentRunError::Session { err } => err.fmt(f),
            UnicastClientComponentRunError::Unicast { err } => err.fmt(f),
            UnicastClientComponentRunError::Start { err } => err.fmt(f),
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
    ) -> Result<(), Error> {
        match self {
            TestCred::IP { addr } => write!(f, "ip://{}", addr),
            TestCred::Unix { addr } => write!(f, "unix://{}", addr)
        }
    }
}

// ISSUE #2: to here
