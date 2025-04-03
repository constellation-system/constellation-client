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
use std::convert::TryFrom;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use clap::ArgMatches;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::authn::TestAuthN;
use constellation_channels::config::CompoundFarEndpoint;
use constellation_channels::far::compound::CompoundFarChannel;
use constellation_channels::far::compound::CompoundFarChannelThreadedFlows;
use constellation_channels::far::compound::CompoundFarChannelXfrm;
use constellation_channels::far::flows::ThreadedFlowsListener;
use constellation_channels::far::registry::CompoundFarChannelRegistry;
use constellation_channels::far::registry::FarChannelRegistryCtx;
use constellation_channels::far::registry::FarChannelRegistryID;
use constellation_channels::far::udp::UDPDatagramXfrm;
use constellation_channels::far::unix::UnixDatagramXfrm;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_channels::unix::UnixSocketAddr;
use constellation_client::component::multicast::CompoundMulticastClientComponent;
use constellation_client::component::multicast::MulticastClientComponent;
use constellation_client::component::multicast::TestCred;
use constellation_client::session::ClientSessionCleanup;
use constellation_client::session::MulticastClientSession;
use constellation_common::error::MutexPoison;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::net::IPEndpointAddr;
use constellation_common::net::SharedMsgs;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::config::PartiesConfig;
use constellation_component_common::xact::XactBatch;
use constellation_component_common::xact::XactBatchCodec;
use constellation_component_common::PartyStreamIdx;
use constellation_standalone::Standalone;
use constellation_standalone::StandaloneApp;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::multicast::StreamMulticasterFrags;
use log::error;
use log::info;
use log::warn;

use crate::config::CmdlineConfig;

pub struct CmdlineSession {
    parties: Arc<RwLock<Vec<PartyStreamIdx>>>
}

#[derive(Clone)]
pub struct CmdlineSessionMsgs {
    parties: Arc<RwLock<Vec<PartyStreamIdx>>>
}

#[derive(Clone)]
pub struct CmdlineSessionRecv;

pub struct CmdlineSessionCleanup;

pub enum CmdlineSessionError {
    SkippedIdx
}

pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

pub type StandaloneRegistry = CompoundFarChannelRegistry<
    Arc<TestAuthN<String, TestCred>>,
    UnixDatagramXfrm,
    UDPDatagramXfrm,
    FarChannelRegistryID
>;

pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches,
    registry: Arc<StandaloneRegistry>
}

pub struct StandaloneCmdline {
    component: CompoundMulticastClientComponent<
        XactBatch<SHA3ID>,
        XactBatch<SHA3ID>,
        PassthruMsgAuthN<XactBatch<SHA3ID>, String>,
        XactBatchCodec<SHA3Algo>,
        SHA3Algo,
        AscendingCount<LargeObjID>,
        CmdlineSessionRecv,
        CmdlineSession,
        AscendingCount<u128>,
        StandaloneCtx
    >
}

impl NSNameCachesCtx for StandaloneCtx {
    /// Exact type of name caches.
    type NameCaches = ThreadedNSNameCaches;

    #[inline]
    fn name_caches(&mut self) -> &mut Self::NameCaches {
        &mut self.caches
    }
}

impl
    FarChannelRegistryCtx<
        CompoundFarChannel,
        CompoundFarChannelThreadedFlows<
            Arc<TestAuthN<String, TestCred>>,
            UnixDatagramXfrm,
            UDPDatagramXfrm,
            FarChannelRegistryID
        >,
        Arc<TestAuthN<String, TestCred>>,
        CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>
    > for StandaloneCtx
{
    #[inline]
    fn far_channel_registry(&mut self) -> Arc<StandaloneRegistry> {
        self.registry.clone()
    }
}

impl SharedMsgs<PartyStreamIdx, XactBatch<SHA3ID>> for CmdlineSessionMsgs {
    /// Type of errors that can occur when collecting messages.
    type MsgsError = MutexPoison;

    /// Collect and report outbound messages.
    ///
    /// This will provide the outbound messages, if there are any, as
    /// well as the time at which to check again for new messages.
    fn msgs(
        &mut self
    ) -> Result<
        (
            Option<Vec<(Vec<PartyStreamIdx>, Vec<LargeObjMsg<SHA3ID>>)>>,
            Option<Instant>
        ),
        Self::MsgsError
    > {
        let guard = self.parties.read().map_err(|_| MutexPoison)?;
        let now = Instant::now();
        let when = now + Duration::from_secs(1);
        let msg = LargeObjMsg::Finish {
            id: 0x0123456789abcdef
        };

        Ok((Some(vec![(guard.clone(), vec![msg])]), Some(when)))
    }
}

impl AuthNMsgRecv<String, XactBatch<SHA3ID>> for CmdlineSessionRecv {
    type RecvError = Infallible;

    fn recv_auth_msg(
        &mut self,
        prin: &String,
        msg: XactBatch<SHA3ID>
    ) -> Result<(), Self::RecvError> {
        info!(target: "cmdline-recv",
              "received message from {}: {:?}",
              prin, msg);

        Ok(())
    }
}

impl
    MulticastClientSession<
        SHA3ID,
        XactBatch<SHA3ID>,
        XactBatch<SHA3ID>,
        PassthruMsgAuthN<XactBatch<SHA3ID>, String>,
        XactBatchCodec<SHA3Algo>,
        AscendingCount<LargeObjID>,
        CmdlineSessionRecv
    > for CmdlineSession
{
    type Cleanup = CmdlineSessionCleanup;
    type Config = ();
    type CreateError = Infallible;
    type StartError = MutexPoison;

    fn create(
        _config: Self::Config
    ) -> Result<
        (
            Self,
            Notify,
            LargeObjProto<
                SHA3ID,
                XactBatch<SHA3ID>,
                XactBatch<SHA3ID>,
                PassthruMsgAuthN<XactBatch<SHA3ID>, String>,
                PartyStreamIdx,
                XactBatchCodec<SHA3Algo>,
                AscendingCount<LargeObjID>,
                CmdlineSessionRecv,
                StreamMulticasterFrags<PartyStreamIdx, OutboundFrags>
            >
        ),
        Self::CreateError
    > {
        let parties = Arc::new(RwLock::new(Vec::new()));

        Ok((
            CmdlineSession {
                parties: parties.clone()
            },
            Notify::new(),
            CmdlineSessionRecv
        ))
    }

    fn start<I>(
        self,
        parties: I
    ) -> Result<Self::Cleanup, Self::StartError>
    where
        I: Iterator<Item = (PartyStreamIdx, String)> {
        let mut guard = self.parties.write().map_err(|_| MutexPoison)?;

        *guard = parties.map(|(idx, _)| idx).collect();

        Ok(CmdlineSessionCleanup)
    }
}

impl ClientSessionCleanup for CmdlineSessionCleanup {
    fn cleanup(self) {}
}

impl Standalone for StandaloneCmdline {
    type Config = CmdlineConfig;
    type CreateCleanup = StandaloneCreateCleanup;

    const CONFIG_FILES: &[&str] = &["cmdline.conf"];
    const NAME: &str = "cmdline";
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn create(
        _args: ArgMatches,
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let (
            name_caches_config,
            registry_config,
            client_config,
            session_config
        ) = config.take();
        let shutdown = ShutdownFlag::new();
        let (mut caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config, shutdown.clone());
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };
        let (listener, reporter) = ThreadedFlowsListener::new();

        // ISSUE #6: This part is temporary, until we get a real
        // authenticator.
        let multicast_config = client_config.multicast();
        let parties_config = multicast_config.parties();
        let authn_parties = match parties_config {
            PartiesConfig::Static { stat } => {
                let mut authn_parties = Vec::with_capacity(stat.len());

                for party in stat {
                    let id = party.party();

                    for conn in party.party_config().connections() {
                        for endpoint in conn.endpoints() {
                            match endpoint {
                                CompoundFarEndpoint::Unix { unix_datagram } => {
                                    match UnixSocketAddr::try_from(
                                        unix_datagram
                                    ) {
                                        Ok(addr) => {
                                            let cred =
                                                TestCred::Unix { addr: addr };

                                            authn_parties
                                                .push((cred, id.clone()));
                                        }
                                        Err(err) => {
                                            warn!(target: "start",
                                              "error converting path: {}",
                                              err);
                                        }
                                    }
                                }
                                CompoundFarEndpoint::UDP { udp } => match udp
                                    .ip_endpoint()
                                {
                                    IPEndpointAddr::Addr(addr) => {
                                        let addr =
                                            SocketAddr::new(*addr, udp.port());
                                        let cred = TestCred::IP { addr: addr };

                                        authn_parties.push((cred, id.clone()));
                                    }
                                    IPEndpointAddr::Name(name) => {
                                        warn!(target: "start",
                                               "discarding endpoint {}",
                                               name);
                                    }
                                }
                            }
                        }
                    }
                }

                authn_parties
            }
        };

        let authn = Arc::new(TestAuthN::create(authn_parties.into_iter()));

        match StandaloneRegistry::create(
            &mut caches,
            authn,
            reporter,
            registry_config
        ) {
            Ok(registry) => {
                let ctx = StandaloneCtx {
                    registry: Arc::new(registry),
                    caches: caches
                };
                let component = MulticastClientComponent::create(
                    client_config,
                    session_config,
                    listener,
                    shutdown,
                    ctx
                );
                let standalone = StandaloneCmdline {
                    component: component
                };

                Ok((standalone, cleanup))
            }
            Err(err) => {
                error!(target: "start",
                       "error creating channel registry: {}",
                       err);

                Err(cleanup)
            }
        }
    }
}

impl StandaloneApp for StandaloneCmdline {
    type RunErrorCleanup = ();

    fn run(
        self,
        _shutdown: ShutdownFlag
    ) -> Result<(), Self::RunErrorCleanup> {
        info!(target: "example",
              "starting example");

        let cleanup = self.component.start().map_err(|err| {
            error!(target: "example",
                       "error starting comonent: {}",
                       err);

            ()
        })?;

        cleanup.cleanup();

        Ok(())
    }

    fn cleanup(_create: Self::CreateCleanup) {}

    fn cleanup_err(
        create: Self::CreateCleanup,
        _run: Self::RunErrorCleanup
    ) {
        let StandaloneCreateCleanup { caches_join, .. } = create;

        if let Err(_) = caches_join.join() {
            error!(target: "example",
                   "error joining caches");
        }
    }
}

impl Display for CmdlineSessionError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            CmdlineSessionError::SkippedIdx => {
                write!(f, "stream parties skipped an index")
            }
        }
    }
}
