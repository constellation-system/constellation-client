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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use constellation_auth::authn::SessionAuthN;
use constellation_auth::authn::TestAuthN;
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
use constellation_common::codec::DatagramCodec;
use constellation_common::ids::IDGen;
use constellation_common::net::DatagramXfrm;
use constellation_common::net::DatagramXfrmCreate;
use constellation_common::net::IPEndpointAddr;
use constellation_common::net::Socket;
use constellation_common::sched::DenseItemID;
use constellation_common::shutdown::ShutdownFlag;
use constellation_component_common::comm::multicast::MulticastComm;
use constellation_component_common::comm::multicast::MulticastCommCleanup;
use constellation_component_common::comm::multicast::MulticastCommRunError;
use constellation_component_common::PartyStreamIdx;
use constellation_streams::addrs::Addrs;
use constellation_streams::addrs::AddrsCreate;
use constellation_streams::channels::ChannelParam;
use constellation_streams::error::ErrorReportInfo;
use constellation_streams::select::StreamSelectorCreateError;
use constellation_streams::select::ThreadedStreamSelectorError;
use constellation_streams::stream::ConcurrentStream;
use constellation_streams::stream::StreamID;
use log::debug;
use log::info;

use crate::config::MulticastClientConfig;
use crate::session::ClientSessionCleanup;
use crate::session::MulticastClientSession;

pub type CompoundMulticastClientComponent<Ctx, Session, Epochs, MsgCodec> =
    MulticastClientComponent<
        Session,
        MsgCodec,
        Epochs,
        CompoundFarChannel,
        CompoundFarChannelThreadedFlows<
            Arc<TestAuthN<String, TestCred>>,
            UnixDatagramXfrm,
            UDPDatagramXfrm,
            FarChannelRegistryID
        >,
        Arc<TestAuthN<String, TestCred>>,
        CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>,
        Ctx,
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>,
        CompoundFarEndpoint
    >;

pub struct MulticastClientComponent<
    Session,
    MsgCodec,
    Epochs,
    Channel,
    F,
    AuthN,
    Xfrm,
    Ctx,
    Resolver,
    Endpoint
> where
    Session: MulticastClientSession<AuthN::Prin>,
    Epochs: 'static + IDGen + Iterator + Send,
    Epochs::Item: Clone + Default + Display + Ord + Send,
    AuthN: Clone
        + SessionAuthN<<Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow>
        + Send
        + Sync,
    AuthN::Prin: 'static + Clone + Display + Eq + Hash + Send,
    MsgCodec: Clone + DatagramCodec<Session::Msg> + Send,
    <MsgCodec as DatagramCodec<Session::Msg>>::Param: Default,
    <MsgCodec as DatagramCodec<Session::Msg>>::EncodeError:
        ErrorReportInfo<DenseItemID<usize>>,
    Channel:
        FarChannelOwnedFlows<F, AuthN, Xfrm> + FarChannelCreate + Send + Sync,
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
    F: OwnedFlowsCreate<Channel::Socket, Channel::Nego, AuthN, Channel::Xfrm>
        + Send,
    F::Flow: 'static + ConcurrentStream + Send,
    F::CreateParam: Clone + Default + Send + Sync,
    F::Reporter: Clone + Send + Sync,
    F::ChannelID: From<usize> + Into<usize> + Send + Sync,
    Xfrm:
        DatagramXfrm + DatagramXfrmCreate<Addr = Channel::Param> + Send + Sync,
    Xfrm::CreateParam: Clone + Default + Send + Sync,
    Xfrm::LocalAddr: From<<Channel::Socket as Socket>::Addr>,
    Ctx: 'static
        + FarChannelRegistryCtx<Channel, F, AuthN, Xfrm>
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
    config: MulticastClientConfig<
        AuthN::Prin,
        ChannelRegistryChannelsConfig<MsgCodec::Param>,
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
        AuthN::Prin
    >,
    shutdown: ShutdownFlag,
    ctx: Ctx
}

pub struct MulticastClientComponentCleanup<Session>
where
    Session: ClientSessionCleanup {
    shutdown: ShutdownFlag,
    multicast: MulticastCommCleanup,
    session: Session
}

#[derive(Debug)]
pub enum MulticastClientComponentRunError<Session, Multicast> {
    Session { err: Session },
    Multicast { err: Multicast },
    SkippedIdx
}

impl<
        Session,
        MsgCodec,
        Epochs,
        Channel,
        F,
        AuthN,
        Xfrm,
        Ctx,
        Resolver,
        Endpoint
    >
    MulticastClientComponent<
        Session,
        MsgCodec,
        Epochs,
        Channel,
        F,
        AuthN,
        Xfrm,
        Ctx,
        Resolver,
        Endpoint
    >
where
    Session: MulticastClientSession<AuthN::Prin>,
    Session::Msg: 'static,
    Session::Msgs: 'static,
    Session::Recv: 'static,
    Epochs: 'static + IDGen + Iterator<Item = u128> + Send + Sync,
    AuthN: 'static
        + Clone
        + SessionAuthN<<Channel::Nego as OwnedFlowNegotiator<F::Flow>>::Flow>
        + Send
        + Sync,
    AuthN::Prin: 'static + Clone + Display + Eq + Hash + Send,
    MsgCodec: 'static + Clone + DatagramCodec<Session::Msg> + Send,
    <MsgCodec as DatagramCodec<Session::Msg>>::Param: Default,
    <MsgCodec as DatagramCodec<Session::Msg>>::EncodeError:
        ErrorReportInfo<DenseItemID<usize>>,
    Channel: 'static
        + FarChannelOwnedFlows<F, AuthN, Xfrm>
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
    F: 'static,
    F: OwnedFlowsCreate<Channel::Socket, Channel::Nego, AuthN, Channel::Xfrm>
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
        + FarChannelRegistryCtx<Channel, F, AuthN, Xfrm>
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
        config: MulticastClientConfig<
            AuthN::Prin,
            ChannelRegistryChannelsConfig<MsgCodec::Param>,
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
            AuthN::Prin
        >,
        shutdown: ShutdownFlag,
        ctx: Ctx
    ) -> Self {
        MulticastClientComponent {
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
        MulticastClientComponentCleanup<Session::Cleanup>,
        MulticastClientComponentRunError<
            Session::CreateError,
            MulticastCommRunError<
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
                MsgCodec::CreateError,
                StreamSelectorCreateError<
                    FarChannelRegistryChannelsCreateError<MsgCodec::CreateError>,
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
            >
        >
    >{
        let MulticastClientComponent {
            session_config,
            config,
            listener,
            ctx,
            shutdown,
            ..
        } = self;

        info!(target: "multicast-client-component",
              "starting multicast client component");

        let (self_party, multicast_config) = config.take();
        let (session, msgs, notify, recv) = Session::create(session_config)
            .map_err(|err| MulticastClientComponentRunError::Session {
                err: err
            })?;
        let multicast: MulticastComm<
            _,
            MsgCodec,
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
        > = MulticastComm::create(
            self_party.clone(),
            multicast_config,
            listener,
            ctx,
            shutdown.clone(),
            notify,
            recv.clone(),
            msgs.clone()
        )
        .map_err(|err| {
            MulticastClientComponentRunError::Multicast { err: err }
        })?;
        let party_data = match multicast.parties() {
            Ok(parties) => {
                let mut parties: Vec<(PartyStreamIdx, AuthN::Prin)> =
                    parties.collect();

                parties.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

                let mut party_data = Vec::with_capacity(parties.len());

                for (idx, party) in parties.into_iter() {
                    let idx: usize = idx.into();

                    if idx == party_data.len() {
                        party_data.push(party)
                    } else {
                        return Err(
                            MulticastClientComponentRunError::SkippedIdx
                        );
                    }
                }

                party_data
            }
        };
        let session_cleanup = session.start(party_data);

        debug!(target: "multicast-client-component",
               "starting multicaster");

        let multicast_cleanup = multicast.start();

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

        shutdown.set();
        multicast.cleanup();
        session.cleanup();
    }
}

impl<Session, Multicast> Display
    for MulticastClientComponentRunError<Session, Multicast>
where
    Session: Display,
    Multicast: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            MulticastClientComponentRunError::Session { err } => err.fmt(f),
            MulticastClientComponentRunError::Multicast { err } => err.fmt(f),
            MulticastClientComponentRunError::SkippedIdx => {
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
