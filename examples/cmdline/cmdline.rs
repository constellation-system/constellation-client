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
use constellation_common::codec::Codec;
use constellation_common::error::ErrorScope;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::net::IPEndpointAddr;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::config::PartiesConfig;
use constellation_component_common::xact::XactBatch;
use constellation_component_common::xact::XactBatchCodec;
use constellation_component_common::xact::XactEffects;
use constellation_component_common::xact::XactSealed;
use constellation_component_common::xact::XactUncommittedReq;
use constellation_component_common::PartyStreamIdx;
use constellation_standalone::Standalone;
use constellation_standalone::StandaloneApp;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::frags::Frags;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::large_obj::LargeObjSender;
use constellation_streams::multicast::StreamMulticasterFrags;
use log::debug;
use log::error;
use log::info;
use log::warn;
use uuid::Uuid;

use crate::config::CmdlineConfig;

#[derive(Clone)]
enum CmdlineMode {
    NoEffects {
        hard: bool,
        correct: bool,
        error: bool
    },
    SoftEmptyEffects {
        correct: bool,
        error: bool
    },
    Effects {
        hard: bool,
        correct: bool,
        error: bool
    }
}

impl CmdlineMode {
    fn req(
        &self,
        uuid: &Uuid,
        version: &Version
    ) -> XactUncommittedReq<u128, TestPayload, TestEffects> {
        match self {
            CmdlineMode::NoEffects {
                hard,
                correct,
                error
            } => {
                let res = if *error {
                    Err(TestError {
                        err: String::from("test error")
                    })
                } else {
                    Ok(TestResult { val: vec![0x10] })
                };
                let payload = if *correct {
                    TestPayload {
                        effects: vec![],
                        res: res
                    }
                } else {
                    TestPayload {
                        effects: vec![0x01],
                        res: res
                    }
                };
                let effects = if *hard {
                    XactEffects::HardNone { when: None }
                } else {
                    XactEffects::SoftNone
                };

                XactUncommittedReq::new(
                    uuid.clone(),
                    version.clone(),
                    0,
                    payload,
                    effects
                )
            }
            CmdlineMode::Effects {
                hard,
                correct,
                error
            } => {
                let res = if *error {
                    Err(TestError {
                        err: String::from("test error")
                    })
                } else {
                    Ok(TestResult { val: vec![0x10] })
                };
                let payload = if *correct {
                    TestPayload {
                        effects: vec![0x01],
                        res: res
                    }
                } else {
                    TestPayload {
                        effects: vec![0x02],
                        res: res
                    }
                };
                let effects = XactEffects::Effects {
                    effects: TestEffects {
                        effects: vec![0x01]
                    },
                    hard: *hard
                };

                XactUncommittedReq::new(
                    uuid.clone(),
                    version.clone(),
                    0,
                    payload,
                    effects
                )
            }
            CmdlineMode::SoftEmptyEffects { correct, error } => {
                let res = if *error {
                    Err(TestError {
                        err: String::from("test error")
                    })
                } else {
                    Ok(TestResult { val: vec![0x10] })
                };
                let payload = if *correct {
                    TestPayload {
                        effects: vec![0x01],
                        res: res
                    }
                } else {
                    TestPayload {
                        effects: vec![0x02],
                        res: res
                    }
                };
                let effects = XactEffects::Effects {
                    effects: TestEffects { effects: vec![] },
                    hard: false
                };

                XactUncommittedReq::new(
                    uuid.clone(),
                    version.clone(),
                    0,
                    payload,
                    effects
                )
            }
        }
    }
}

pub struct CmdlineSession {
    parties: Arc<RwLock<Vec<PartyStreamIdx>>>
}

#[derive(Clone)]
pub struct CmdlineSessionMsgs {
    mode: CmdlineMode,
    version: Version,
    hash: SHA3Algo,
    when: Instant,
    uuid: Uuid,
    count: u64
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
        TestBatch,
        TestBatch,
        PassthruMsgAuthN<TestBatch, String>,
        TestBatchCodec,
        SHA3Algo,
        AscendingCount<LargeObjID>,
        CmdlineSessionMsgs,
        CmdlineSessionRecv,
        CmdlineSession,
        AscendingCount<u128>,
        StandaloneCtx
    >
}

impl Default for CmdlineSessionRecv {
    #[inline]
    fn default() -> Self {
        CmdlineSessionRecv
    }
}

impl CmdlineSessionMsgs {
    #[inline]
    fn new(
        mode: CmdlineMode,
        hash: SHA3Algo
    ) -> Self {
        let uuid =
            Uuid::new_v5(&Uuid::NAMESPACE_DNS, TEST_SERVICE_NAME.as_bytes());

        debug!(target: "cmdline-msgs",
               "service UUID: {}",
               uuid);

        CmdlineSessionMsgs {
            when: Instant::now(),
            version: TEST_VERSION,
            mode: mode,
            uuid: uuid,
            hash: hash,
            count: 0
        }
    }
}

impl CmdlineSession {
    #[inline]
    fn new() -> Self {
        CmdlineSession {
            parties: Arc::new(RwLock::new(Vec::new()))
        }
    }
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

impl LargeObjMsgs<SHA3Algo, TestBatch> for CmdlineSessionMsgs {
    type AddMsgsError<Encode>
        = LargeObjProtoAddOutboundError<SHA3ID, Encode>
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
        let now = Instant::now();

        if now >= self.when {
            debug!(target: "cmdline-msgs",
                   "generating outgoing batch, seqnum {}",
                   self.count);

            let req = self.mode.req(&self.uuid, &self.version);
            let sealed = XactSealed::new(TestSeal, req);
            let batch = XactBatch::new(vec![], vec![sealed], vec![]);

            sender.add_outbound(&batch)?;
            self.count += 1;

            self.when = now + Duration::from_secs(5);
        }

        Ok(Some(self.when))
    }
}

impl AuthNMsgRecv<String, TestBatch> for CmdlineSessionRecv {
    type RecvError = Infallible;

    fn recv_auth_msg(
        &mut self,
        prin: &String,
        msg: TestBatch
    ) -> Result<(), Self::RecvError> {
        info!(target: "cmdline-recv",
              "received batch from {}: {:?}",
              prin, msg);

        for _ in msg.uncommitted() {
            warn!(target: "cmdline-recv",
                  "discarding uncommitted request")
        }

        for _ in msg.committed() {
            warn!(target: "cmdline-recv",
                  "discarding committed round")
        }

        for notify in msg.notifies() {
            info!(target: "cmdline-recv",
                  "notification for {}: {}",
                  notify.hash(), notify.state())
        }

        Ok(())
    }
}

impl
    MulticastClientSession<
        SHA3Algo,
        TestBatch,
        TestBatch,
        PassthruMsgAuthN<TestBatch, String>,
        TestBatchCodec,
        AscendingCount<LargeObjID>,
        CmdlineSessionMsgs,
        CmdlineSessionRecv
    > for CmdlineSession
{
    type Cleanup = CmdlineSessionCleanup;
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
                PassthruMsgAuthN<TestBatch, String>,
                PartyStreamIdx,
                TestBatchCodec,
                AscendingCount<LargeObjID>,
                CmdlineSessionMsgs,
                CmdlineSessionRecv,
                StreamMulticasterFrags<PartyStreamIdx, OutboundFrags>
            >
        ),
        Self::CreateError
    > {
        let hash = SHA3Algo::default();
        let authn = PassthruMsgAuthN::default();
        let msgs = CmdlineSessionMsgs::new(
            CmdlineMode::NoEffects {
                hard: true,
                correct: true,
                error: false
            },
            hash.clone()
        );
        let recv = CmdlineSessionRecv::default();
        let notify = Notify::new();
        let proto = LargeObjProto::create(
            config,
            notify.clone(),
            recv,
            msgs,
            authn,
            hash
        )?;
        let session = CmdlineSession::new();

        Ok((session, notify, proto))
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

type TestBatch = XactBatch<
    u128,
    SHA3ID,
    TestSeal,
    TestPayload,
    TestEffects,
    TestResult,
    TestError
>;
type TestBatchCodec = XactBatchCodec<
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
