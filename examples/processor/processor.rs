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

use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;
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
use constellation_common::error::ErrorScope;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::xact::XactBatchHashCodec;
use constellation_component_common::xact::XactCommittedReq;
use constellation_component_common::xact::XactCommittedRound;
use constellation_component_common::xact::XactEffects;
use constellation_component_common::xact::XactError;
use constellation_component_common::xact::XactHashBatch;
use constellation_component_common::xact::XactLinPoint;
use constellation_component_common::xact::XactNotify;
use constellation_component_common::xact::XactNotifyState;
use constellation_component_common::xact::XactSealed;
use constellation_component_common::xact::XactUncommittedHashReq;
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
use log::warn;
use uuid::Uuid;

use crate::config::ProcessorConfig;

pub struct ProcessorSession;

#[derive(Clone)]
pub struct ProcessorSessionMsgs {
    pending: Arc<Mutex<Vec<XactNotify<u128, SHA3ID, TestResult, TestError>>>>,
    // XXX this is a hack to get the demo working; replace with a
    // processor capability reporting message of some kind.
    first: bool
}

#[derive(Clone)]
pub struct ProcessorSessionRecv {
    class: Uuid,
    version: Version,
    pending: Arc<Mutex<Vec<XactNotify<u128, SHA3ID, TestResult, TestError>>>>,
    when: XactLinPoint<u128>,
    notify: Notify
}

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
        TestBatch,
        TestBatch,
        PassthruMsgAuthN<TestBatch, TestCred>,
        TestBatchCodec,
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
    fn new(
        pending: Arc<
            Mutex<Vec<XactNotify<u128, SHA3ID, TestResult, TestError>>>
        >
    ) -> Self {
        ProcessorSessionMsgs {
            pending: pending,
            first: true
        }
    }
}

impl ProcessorSessionRecv {
    #[inline]
    fn new(
        class: Uuid,
        version: Version,
        pending: Arc<
            Mutex<Vec<XactNotify<u128, SHA3ID, TestResult, TestError>>>
        >,
        notify: Notify
    ) -> Self {
        ProcessorSessionRecv {
            class: class,
            version: version,
            pending: pending,
            notify: notify,
            when: XactLinPoint::new(0, 0)
        }
    }
}

impl ProcessorSessionMsgs {
    fn get_msgs(
        &self
    ) -> Result<Vec<XactNotify<u128, SHA3ID, TestResult, TestError>>, MutexPoison>
    {
        let mut guard = self.pending.lock().map_err(|_| MutexPoison)?;

        Ok(std::mem::replace(guard.deref_mut(), Vec::new()))
    }
}

impl LargeObjMsgs<SHA3Algo, TestBatch> for ProcessorSessionMsgs {
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<SHA3ID, Encode>>
    where
        Encode: Display + ScopedError;

    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<SHA3Algo, TestBatch, WrapperCodec, F>
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Codec<TestBatch>,
        WrapperCodec::Param: Default,
        F: Frags {
        let msgs = self.get_msgs()?;

        if self.first || msgs.len() != 0 {
            debug!(target: "processor-session-msgs",
                   "sending {} notifies",
                   msgs.len());

            let batch = XactHashBatch::new(vec![], vec![], msgs);

            self.first = false;
            sender
                .add_outbound(&batch)
                .map_err(|err| WithMutexPoison::Inner { error: err })?;
        }

        Ok(None)
    }
}

impl ProcessorSessionRecv {
    fn run_uncommitted(
        &self,
        req: XactSealed<
            TestSeal,
            XactUncommittedHashReq<u128, SHA3ID, TestPayload, TestEffects>
        >
    ) -> XactNotify<u128, SHA3ID, TestResult, TestError> {
        let (_, req) = req.take();
        let (class, version, instance, hash, effects, payload) = req.take();

        info!(target: "processor-recv",
              "processing transaction {}",
              hash);

        let res = if class != self.class {
            debug!(target: "processor-recv",
                  "bad transaction class {}",
                   class);

            Err(XactError::UnknownClass)
        } else if version != self.version {
            debug!(target: "processor-recv",
                  "bad transaction version {}",
                   version);

            Err(XactError::UnknownVersion)
        } else if instance != 0 {
            debug!(target: "processor-recv",
                  "bad transaction instance {}",
                   instance);

            Err(XactError::UnknownInstance)
        } else {
            match effects {
                XactEffects::Effects { .. } => {
                    debug!(target: "processor-recv",
                           "transaction reports effects but is uncommitted");

                    Err(XactError::Uncommitted)
                }
                XactEffects::HardNone { .. } | XactEffects::SoftNone => {
                    debug!(target: "processor-recv",
                           "transaction reports no effects");

                    if payload.effects.is_empty() {
                        debug!(target: "processor-recv",
                               "generating normal result");

                        payload.res.map_err(|err| XactError::Error { err: err })
                    } else {
                        debug!(target: "processor-recv",
                               "transaction reports causes effects");

                        Err(XactError::Uncommitted)
                    }
                }
            }
        };

        match res {
            Ok(res) => {
                let state = XactNotifyState::Success {
                    result: Some(res),
                    when: self.when.clone()
                };

                XactNotify::new(hash, state)
            }
            Err(err) => {
                let state = XactNotifyState::Error { error: Some(err) };

                XactNotify::new(hash, state)
            }
        }
    }

    fn run_committed(
        &self,
        hash: SHA3ID,
        req: XactCommittedReq<TestPayload, TestEffects>
    ) -> XactNotify<u128, SHA3ID, TestResult, TestError> {
        let (class, version, instance, _, effects, payload) = req.take();

        info!(target: "processor-recv",
              "processing transaction {}",
              hash);

        let res = if class != self.class {
            debug!(target: "processor-recv",
                  "bad transaction class {}",
                   class);

            Err(XactError::UnknownClass)
        } else if version != self.version {
            debug!(target: "processor-recv",
                  "bad transaction version {}",
                   version);

            Err(XactError::UnknownVersion)
        } else if instance != 0 {
            debug!(target: "processor-recv",
                  "bad transaction instance {}",
                   instance);

            Err(XactError::UnknownInstance)
        } else {
            match effects {
                Some(effects) if effects.hard() => {
                    debug!(target: "processor-recv",
                           "transaction has effects");

                    let expected: HashSet<u8> =
                        effects.effects().effects.iter().cloned().collect();
                    let actual: HashSet<u8> =
                        payload.effects.iter().cloned().collect();

                    if actual.is_subset(&expected) {
                        payload.res.map_err(|err| XactError::Error { err: err })
                    } else {
                        Err(XactError::EffectViolation)
                    }
                }
                _ => {
                    debug!(target: "processor-recv",
                           "generating result");

                    payload.res.map_err(|err| XactError::Error { err: err })
                }
            }
        };

        match res {
            Ok(res) => {
                let state = XactNotifyState::Success {
                    result: Some(res),
                    when: self.when.clone()
                };

                XactNotify::new(hash, state)
            }
            Err(err) => {
                let state = XactNotifyState::Error { error: Some(err) };

                XactNotify::new(hash, state)
            }
        }
    }

    fn process_uncommitted_reqs(
        &self,
        reqs: Vec<
            XactSealed<
                TestSeal,
                XactUncommittedHashReq<u128, SHA3ID, TestPayload, TestEffects>
            >
        >
    ) -> Result<(), MutexPoison> {
        info!(target: "processor-recv",
              "processing uncommitted transactions");

        for req in reqs {
            let res = self.run_uncommitted(req);
            let mut guard = self.pending.lock().map_err(|_| MutexPoison)?;

            guard.push(res);
            self.notify.notify().map_err(|_| MutexPoison)?;
        }

        Ok(())
    }

    fn process_committed_rounds(
        &self,
        rounds: Vec<
            XactCommittedRound<
                u128,
                SHA3ID,
                TestSeal,
                TestPayload,
                TestEffects
            >
        >
    ) -> Result<(), MutexPoison> {
        let mut codec = TestBatchCodec::create(((), (), (), (), ()))
            .expect("Expected success");

        for round in rounds {
            let (id, seal, reqs) = round.take();

            info!(target: "processor-recv",
                  "processing transactions from round {}",
                  id);

            if let Some(seal) = seal {
                debug!(target: "processor-recv",
                       "checking round {} against consensus seal",
                       id);

                for req in reqs.into_iter() {
                    match codec.hash_committed(&req) {
                        Ok(hash) => {
                            let hashes = seal.hashes();
                            let idx = req.idx() as usize;

                            if hashes.len() <= idx && hashes[idx] == hash {
                                let res = self.run_committed(hash, req);

                                self.pending
                                    .lock()
                                    .map_err(|_| MutexPoison)?
                                    .push(res);
                                self.notify
                                    .notify()
                                    .map_err(|_| MutexPoison)?;
                            } else {
                                let state = XactNotifyState::Error {
                                    error: Some(XactError::HashMismatch)
                                };
                                let res = XactNotify::new(hash, state);

                                self.pending
                                    .lock()
                                    .map_err(|_| MutexPoison)?
                                    .push(res);
                                self.notify
                                    .notify()
                                    .map_err(|_| MutexPoison)?;
                            }
                        }
                        Err(err) => {
                            debug!(target: "processor-recv",
                                   "error computing hash: {}",
                                   err);
                        }
                    }
                }
            } else {
                debug!(target: "processor-recv",
                       "round {} has no consensus seal",
                       id);

                for req in reqs.into_iter() {
                    match codec.hash_committed(&req) {
                        Ok(hash) => {
                            let res = self.run_committed(hash, req);

                            self.pending
                                .lock()
                                .map_err(|_| MutexPoison)?
                                .push(res);
                            self.notify.notify().map_err(|_| MutexPoison)?;
                        }
                        Err(err) => {
                            debug!(target: "processor-recv",
                                   "error computing hash: {}",
                                   err);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl AuthNMsgRecv<TestCred, TestBatch> for ProcessorSessionRecv {
    type RecvError = MutexPoison;

    fn recv_auth_msg(
        &mut self,
        prin: &TestCred,
        msg: TestBatch
    ) -> Result<(), Self::RecvError> {
        let (committed, uncommitted, notifies) = msg.take();

        info!(target: "processor-recv",
              "received batch from {}",
              prin);

        self.process_uncommitted_reqs(uncommitted)?;
        self.process_committed_rounds(committed)?;

        for notify in notifies {
            warn!(target: "processor-recv",
                  "discarded notification for {}: {}",
                  notify.hash(), notify.state())
        }

        Ok(())
    }
}

impl
    UnicastClientSession<
        SHA3Algo,
        TestBatch,
        TestBatch,
        PassthruMsgAuthN<TestBatch, TestCred>,
        TestBatchCodec,
        AscendingCount<LargeObjID>,
        ProcessorSessionMsgs,
        ProcessorSessionRecv
    > for ProcessorSession
{
    type Cleanup = ProcessorSessionCleanup;
    type Config = LargeObjProtoConfig<((), (), (), (), ()), ()>;
    type CreateError = LargeObjProtoCreateError<
        <TestBatchCodec as Codec<TestBatch>>::CreateError
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
                TestBatch,
                TestBatch,
                PassthruMsgAuthN<TestBatch, TestCred>,
                (),
                TestBatchCodec,
                AscendingCount<LargeObjID>,
                ProcessorSessionMsgs,
                ProcessorSessionRecv,
                OutboundFrags
            >
        ),
        Self::CreateError
    > {
        let class =
            Uuid::new_v5(&Uuid::NAMESPACE_DNS, TEST_SERVICE_NAME.as_bytes());
        let version = Version::new(0, 0, 0);
        let hash = SHA3Algo::default();
        let authn = PassthruMsgAuthN::default();
        let pending = Arc::new(Mutex::new(Vec::new()));
        let notify = Notify::new();
        let msgs = ProcessorSessionMsgs::new(pending.clone());
        let recv =
            ProcessorSessionRecv::new(class, version, pending, notify.clone());
        let proto = LargeObjProto::create(
            config,
            notify.clone(),
            recv,
            msgs,
            authn,
            hash
        )?;

        Ok((ProcessorSession, notify, proto))
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

const TEST_SERVICE_NAME: &str = "org.constellation.test";
const TEST_VERSION: Version = Version::new(0, 0, 0);

#[derive(Clone, Debug)]
pub struct TestEffects {
    effects: Vec<u8>
}

#[derive(Clone, Debug)]
pub struct TestPayload {
    effects: Vec<u8>,
    res: Result<TestResult, TestError>
}

#[derive(Clone, Debug)]
pub struct TestResult {
    val: Vec<u8>
}

#[derive(Clone, Debug)]
pub struct TestError {
    err: String
}

#[derive(Clone, Debug)]
pub struct TestSeal;

#[derive(Clone)]
pub struct TestEffectsCodec;

#[derive(Clone)]
pub struct TestPayloadCodec;

#[derive(Clone)]
pub struct TestResultCodec;

#[derive(Clone)]
pub struct TestErrorCodec;

#[derive(Clone)]
pub struct TestSealCodec;

pub struct TestStringError;

type TestBatch = XactHashBatch<
    u128,
    SHA3ID,
    TestSeal,
    TestPayload,
    TestEffects,
    TestResult,
    TestError
>;
type TestBatchCodec = XactBatchHashCodec<
    u128,
    SHA3Algo,
    TestSeal,
    TestPayload,
    TestEffects,
    TestResult,
    TestError,
    TestSealCodec,
    TestPayloadCodec,
    TestEffectsCodec,
    TestResultCodec,
    TestErrorCodec
>;

impl Codec<TestSeal> for TestSealCodec {
    type CreateError = Infallible;
    type DecodeError = Infallible;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(TestSealCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        _val: &TestSeal
    ) -> usize {
        0
    }

    #[inline]
    fn encode(
        &mut self,
        _val: &TestSeal,
        _buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        Ok(0)
    }

    #[inline]
    fn decode(
        &mut self,
        _buf: &[u8]
    ) -> Result<(TestSeal, usize), Self::DecodeError> {
        Ok((TestSeal, 0))
    }
}

impl Codec<TestEffects> for TestEffectsCodec {
    type CreateError = Infallible;
    type DecodeError = Infallible;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(TestEffectsCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        val: &TestEffects
    ) -> usize {
        val.effects.len() + 1
    }

    #[inline]
    fn encode(
        &mut self,
        val: &TestEffects,
        buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        let len = val.effects.len();

        buf[0] = len as u8;
        buf[1..len + 1].copy_from_slice(&val.effects[..]);

        Ok(len + 1)
    }

    #[inline]
    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(TestEffects, usize), Self::DecodeError> {
        let len = buf[0] as usize;
        let effects = buf[1..len + 1].to_vec();

        Ok((TestEffects { effects: effects }, len + 1))
    }
}

impl Codec<TestResult> for TestResultCodec {
    type CreateError = Infallible;
    type DecodeError = Infallible;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(TestResultCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        val: &TestResult
    ) -> usize {
        val.val.len() + 1
    }

    #[inline]
    fn encode(
        &mut self,
        val: &TestResult,
        buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        let len = val.val.len();

        buf[0] = len as u8;
        buf[1..len + 1].copy_from_slice(&val.val[..]);

        Ok(len + 1)
    }

    #[inline]
    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(TestResult, usize), Self::DecodeError> {
        let len = buf[0] as usize;
        let val = buf[1..len + 1].to_vec();

        Ok((TestResult { val: val }, len + 1))
    }
}

impl Codec<TestError> for TestErrorCodec {
    type CreateError = Infallible;
    type DecodeError = TestStringError;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(TestErrorCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        val: &TestError
    ) -> usize {
        val.err.as_bytes().len() + 1
    }

    #[inline]
    fn encode(
        &mut self,
        val: &TestError,
        buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        let bytes = val.err.as_bytes();
        let len = bytes.len();

        buf[0] = len as u8;
        buf[1..len + 1].copy_from_slice(&bytes[..]);

        Ok(len + 1)
    }

    #[inline]
    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(TestError, usize), Self::DecodeError> {
        let len = buf[0] as usize;
        let val = buf[1..len + 1].to_vec();
        let err = String::from_utf8(val).map_err(|_| TestStringError)?;

        Ok((TestError { err: err }, len + 1))
    }
}

impl Codec<TestPayload> for TestPayloadCodec {
    type CreateError = Infallible;
    type DecodeError = <TestErrorCodec as Codec<TestError>>::DecodeError;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(TestPayloadCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        val: &TestPayload
    ) -> usize {
        let effects = val.effects.len() + 1;
        let result = match &val.res {
            Ok(res) => TestResultCodec.buf_size(res) + 1,
            Err(err) => TestErrorCodec.buf_size(err) + 1
        };

        effects + result
    }

    #[inline]
    fn encode(
        &mut self,
        val: &TestPayload,
        buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        let effects_len = val.effects.len();

        buf[0] = effects_len as u8;
        buf[1..effects_len + 1].copy_from_slice(&val.effects[..]);

        let res_len = match &val.res {
            Ok(res) => {
                buf[effects_len + 1] = 0;

                let Ok(len) =
                    TestResultCodec.encode(res, &mut buf[effects_len + 2..]);

                len + 1
            }
            Err(err) => {
                buf[effects_len + 1] = 1;

                let Ok(len) =
                    TestErrorCodec.encode(err, &mut buf[effects_len + 2..]);

                len + 1
            }
        };

        Ok(effects_len + res_len + 1)
    }

    #[inline]
    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(TestPayload, usize), Self::DecodeError> {
        debug!(target: "processor",
               "decoding {} bytes, {:?}",
               buf.len(), buf);

        let effects_len = buf[0] as usize;
        let effects = buf[1..effects_len + 1].to_vec();

        let (res, res_len) = if buf[effects_len + 1] == 0 {
            let Ok((res, len)) =
                TestResultCodec.decode(&buf[effects_len + 2..]);

            (Ok(res), len)
        } else {
            let (err, len) = TestErrorCodec.decode(&buf[effects_len + 2..])?;

            (Err(err), len)
        };

        Ok((
            TestPayload {
                effects: effects,
                res: res
            },
            effects_len + res_len + 2
        ))
    }
}

impl ScopedError for TestStringError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        ErrorScope::Unrecoverable
    }
}

impl Display for TestResult {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "result: ")?;

        for byte in self.val.iter() {
            write!(f, "{:02x}", byte)?;
        }

        Ok(())
    }
}

impl Display for TestError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "{}", self)
    }
}

impl Display for TestStringError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "bad UTF-8 data")
    }
}
