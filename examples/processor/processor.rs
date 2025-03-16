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
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use clap::ArgMatches;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::TrivialAuthN;
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
use constellation_client::component::unicast::CompoundUnicastClientComponent;
use constellation_client::component::unicast::UnicastClientComponent;
use constellation_client::component::unicast::UnicastClientComponentCleanup;
use constellation_client::component::unicast::TestCred;
use constellation_client::session::ClientSessionCleanup;
use constellation_client::session::UnicastClientSession;
use constellation_common::error::MutexPoison;
use constellation_common::ids::AscendingCount;
use constellation_common::net::PrivateMsgs;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_standalone::Standalone;
use constellation_standalone::StandaloneService;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjMsgCodec;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::config::ProcessorConfig;

pub struct ProcessorSession;

#[derive(Clone)]
pub struct ProcessorSessionMsgs;

#[derive(Clone)]
pub struct ProcessorSessionRecv;

pub struct ProcessorSessionCleanup;

pub type ProcessorCleanup = UnicastClientComponentCleanup<ProcessorSessionCleanup>;

pub enum ProcessorSessionError {
    SkippedIdx
}

pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

pub type StandaloneRegistry = CompoundFarChannelRegistry<
    TrivialAuthN<TestCred>,
    UnixDatagramXfrm,
    UDPDatagramXfrm,
    FarChannelRegistryID
>;

pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches,
    registry: Arc<StandaloneRegistry>
}

pub struct StandaloneProcessor {
    component: CompoundUnicastClientComponent<
        StandaloneCtx,
        ProcessorSession,
        AscendingCount,
        LargeObjMsgCodec
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
            TrivialAuthN<TestCred>,
            UnixDatagramXfrm,
            UDPDatagramXfrm,
            FarChannelRegistryID
        >,
        TrivialAuthN<TestCred>,
        CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>
    > for StandaloneCtx
{
    #[inline]
    fn far_channel_registry(&mut self) -> Arc<StandaloneRegistry> {
        self.registry.clone()
    }
}

impl PrivateMsgs<LargeObjMsg> for ProcessorSessionMsgs {
    /// Type of errors that can occur when collecting messages.
    type MsgsError = MutexPoison;

    /// Collect and report outbound messages.
    ///
    /// This will provide the outbound messages, if there are any, as
    /// well as the time at which to check again for new messages.
    fn msgs(
        &mut self
    ) -> Result<
        (Option<Vec<LargeObjMsg>>, Option<Instant>),
        Self::MsgsError
    > {
        let now = Instant::now();
        let when = now + Duration::from_secs(1);
        let msg = LargeObjMsg::Finish { id: 0x0123456789abcdef };

        trace!(target: "processor-msgs",
               "gathering messages");

        Ok((Some(vec![msg]), Some(when)))
    }
}

impl AuthNMsgRecv<TestCred, LargeObjMsg> for ProcessorSessionRecv {
    type RecvError = Infallible;

    fn recv_auth_msg(
        &mut self,
        prin: &TestCred,
        msg: LargeObjMsg
    ) -> Result<(), Self::RecvError> {
        info!(target: "processor-recv",
              "received message from {}: {:?}",
              prin, msg);

        Ok(())
    }
}

impl UnicastClientSession<TestCred> for ProcessorSession {
    type Config = ();
    type CreateError = Infallible;
    type StartError = MutexPoison;
    type Msg = LargeObjMsg;
    type Msgs = ProcessorSessionMsgs;
    type Recv = ProcessorSessionRecv;
    type Cleanup = ProcessorSessionCleanup;

    fn create(
        _config: Self::Config,
    ) -> Result<(Self, Self::Msgs, Notify, Self::Recv), Self::CreateError> {
        Ok((ProcessorSession,
            ProcessorSessionMsgs,
            Notify::new(),
            ProcessorSessionRecv
        ))
    }

    fn start(
        self,
    ) -> Result<Self::Cleanup, Self::StartError> {
        Ok(ProcessorSessionCleanup)
    }
}

impl ClientSessionCleanup for ProcessorSessionCleanup {
    fn cleanup(self) {
    }
}

impl Standalone for StandaloneProcessor {
    type Config = ProcessorConfig;
    type CreateCleanup = StandaloneCreateCleanup;

    const CONFIG_FILES: &[&str] = &["processor.conf"];
    const NAME: &str = "processor";
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn create(
        _args: ArgMatches,
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let (name_caches_config, registry_config,
             client_config, session_config) = config.take();
        let shutdown = ShutdownFlag::new();
        let (mut caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config, shutdown.clone());
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };
        let (listener, reporter) = ThreadedFlowsListener::new();
        let authn = TrivialAuthN::default();

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
                let component = UnicastClientComponent::create(
                    client_config,
                    session_config,
                    listener,
                    shutdown,
                    ctx
                );
                let standalone = StandaloneProcessor {
                    component: component
                };


                Ok((standalone, cleanup))
            }
            Err(err) => {
                error!(target: "processor-start",
                       "error creating channel registry: {}",
                       err);

                Err(cleanup)
            }
        }
    }
}

impl StandaloneService for StandaloneProcessor {
    type RunCleanup = ProcessorCleanup;
    type RunErrorCleanup = ();

    fn run(self) -> Result<Self::RunCleanup, Self::RunErrorCleanup> {
        match self.component.start() {
            Ok(out) => Ok(out),
            Err(err) => {
                error!(target: "processor",
                       "{}", err);

                Err(())
            }
        }
    }

    fn shutdown(
        mut create_cleanup: Self::CreateCleanup,
        run_cleanup: Option<Self::RunCleanup>
    ) {
        debug!(target: "processor",
               "cleaning up consensus");

        create_cleanup.shutdown.set();

        if let Some(cleanup) = run_cleanup {
            debug!(target: "processor",
               "cleaning up runtime");

            cleanup.cleanup();
        }

        debug!(target: "processor",
               "cleaning up caches");

        if create_cleanup.caches_join.join().is_err() {
            error!(target: "processor",
                   "error shutting down name chache threads")
        }
    }

    fn shutdown_err(
        create: Self::CreateCleanup,
        _run: Self::RunErrorCleanup
    ) {
        let StandaloneCreateCleanup {
            caches_join,
            ..
        } = create;

        if let Err(_) = caches_join.join() {
            error!(target: "processor",
                   "error joining caches");

        }
    }
}

impl Display for ProcessorSessionError
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ProcessorSessionError::SkippedIdx => {
                write!(f, "stream parties skipped an index")
            }
        }
    }
}
