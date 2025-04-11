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
use std::iter::once;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use clap::ArgMatches;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::PassthruMsgAuthN;
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
use constellation_client::component::unicast::TestCred;
use constellation_client::component::unicast::UnicastClientComponent;
use constellation_client::component::unicast::UnicastClientComponentCleanup;
use constellation_client::session::ClientSessionCleanup;
use constellation_client::session::UnicastClientSession;
use constellation_common::codec::Codec;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::xact::XactBatch;
use constellation_component_common::xact::XactBatchCodec;
use constellation_standalone::Standalone;
use constellation_standalone::StandaloneService;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::frags::Frags;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::large_obj::LargeObjSender;
use log::debug;
use log::error;
use log::info;

use crate::config::ProcessorConfig;

pub struct ProcessorSession;

#[derive(Clone)]
pub struct ProcessorSessionMsgs {
    hash: SHA3Algo,
    when: Instant,
    count: u64
}

#[derive(Clone)]
pub struct ProcessorSessionRecv;

pub struct ProcessorSessionCleanup;

pub type ProcessorCleanup =
    UnicastClientComponentCleanup<ProcessorSessionCleanup>;

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
        XactBatch<SHA3ID>,
        XactBatch<SHA3ID>,
        PassthruMsgAuthN<XactBatch<SHA3ID>, TestCred>,
        XactBatchCodec<SHA3Algo>,
        SHA3Algo,
        AscendingCount<LargeObjID>,
        ProcessorSessionMsgs,
        ProcessorSessionRecv,
        ProcessorSession,
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

impl ProcessorSessionMsgs {
    #[inline]
    fn new(hash: SHA3Algo) -> Self {
        ProcessorSessionMsgs {
            when: Instant::now(),
            hash: hash,
            count: 0
        }
    }
}

impl Default for ProcessorSessionRecv {
    #[inline]
    fn default() -> Self {
        ProcessorSessionRecv
    }
}

impl LargeObjMsgs<SHA3Algo, XactBatch<SHA3ID>> for ProcessorSessionMsgs {
    type AddMsgsError<Encode>
        = LargeObjProtoAddOutboundError<SHA3ID, Encode>
    where
        Encode: Display + ScopedError;

    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<
            SHA3Algo,
            XactBatch<SHA3ID>,
            WrapperCodec,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Codec<XactBatch<SHA3ID>>,
        WrapperCodec::Param: Default,
        F: Frags {
        let now = Instant::now();

        if self.when <= now {
            debug!(target: "processor-msgs",
                   "generating outgoing batch, seqnum {}",
                   self.count);

            let batch = XactBatch::create(
                &self.hash,
                self.count,
                once(vec![0x22; 10000])
            );

            sender.add_outbound(&batch)?;
            self.count += 1;

            self.when = now + Duration::from_secs(5);
        }

        Ok(Some(self.when))
    }
}

impl AuthNMsgRecv<TestCred, XactBatch<SHA3ID>> for ProcessorSessionRecv {
    type RecvError = Infallible;

    fn recv_auth_msg(
        &mut self,
        prin: &TestCred,
        msg: XactBatch<SHA3ID>
    ) -> Result<(), Self::RecvError> {
        info!(target: "processor-recv",
              "received message from {}: {:?}",
              prin, msg);

        Ok(())
    }
}

impl
    UnicastClientSession<
        SHA3Algo,
        XactBatch<SHA3ID>,
        XactBatch<SHA3ID>,
        PassthruMsgAuthN<XactBatch<SHA3ID>, TestCred>,
        XactBatchCodec<SHA3Algo>,
        AscendingCount<LargeObjID>,
        ProcessorSessionMsgs,
        ProcessorSessionRecv
    > for ProcessorSession
{
    type Cleanup = ProcessorSessionCleanup;
    type Config = LargeObjProtoConfig<(), ()>;
    type CreateError = LargeObjProtoCreateError<
        <XactBatchCodec<SHA3Algo> as Codec<XactBatch<SHA3ID>>>::CreateError
    >;
    type StartError = MutexPoison;

    fn create(
        config: Self::Config
    ) -> Result<
        (
            Self,
            Notify,
            LargeObjProto<
                SHA3Algo,
                XactBatch<SHA3ID>,
                XactBatch<SHA3ID>,
                PassthruMsgAuthN<XactBatch<SHA3ID>, TestCred>,
                (),
                XactBatchCodec<SHA3Algo>,
                AscendingCount<LargeObjID>,
                ProcessorSessionMsgs,
                ProcessorSessionRecv,
                OutboundFrags
            >
        ),
        Self::CreateError
    > {
        let hash = SHA3Algo::default();
        let authn = PassthruMsgAuthN::default();
        let msgs = ProcessorSessionMsgs::new(hash.clone());
        let recv = ProcessorSessionRecv::default();
        let proto = LargeObjProto::create(config, recv, msgs, authn, hash)?;

        Ok((ProcessorSession, Notify::new(), proto))
    }

    fn start(self) -> Result<Self::Cleanup, Self::StartError> {
        Ok(ProcessorSessionCleanup)
    }
}

impl ClientSessionCleanup for ProcessorSessionCleanup {
    fn cleanup(self) {}
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
               "cleaning up processor");

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
        let StandaloneCreateCleanup { caches_join, .. } = create;

        if let Err(_) = caches_join.join() {
            error!(target: "processor",
                   "error joining caches");
        }
    }
}

impl Display for ProcessorSessionError {
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
