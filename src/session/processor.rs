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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Display;
use std::io::Error;
use std::marker::PhantomData;
#[cfg(feature = "standalone")]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(feature = "standalone")]
use std::thread::JoinHandle;
use std::time::Instant;

#[cfg(feature = "standalone")]
use clap::ArgMatches;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNedDestruct;
#[cfg(feature = "standalone")]
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashID;
use constellation_common::retry::Retry;
#[cfg(feature = "standalone")]
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
#[cfg(feature = "standalone")]
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
#[cfg(feature = "standalone")]
use constellation_common::version::VersionSuffix;
use constellation_component_common::config::XactBatchCodecConfig;
use constellation_component_common::xact::XactBatchCodecCreateError;
use constellation_component_common::xact::XactCommittedEffects;
use constellation_component_common::xact::XactCommittedReq;
use constellation_component_common::xact::XactCommittedRound;
use constellation_component_common::xact::XactCommittedRoundCodecCreateError;
use constellation_component_common::xact::XactEffects;
use constellation_component_common::xact::XactError;
use constellation_component_common::xact::XactHashBatch;
use constellation_component_common::xact::XactHashBatchCodec;
use constellation_component_common::xact::XactLinPoint;
use constellation_component_common::xact::XactNotify;
use constellation_component_common::xact::XactNotifyCodecCreateError;
use constellation_component_common::xact::XactNotifyState;
use constellation_component_common::xact::XactReqCodecCreateError;
use constellation_component_common::xact::XactSealed;
use constellation_component_common::xact::XactSealedCodecCreateError;
use constellation_component_common::xact::XactUncommittedHashReq;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
#[cfg(feature = "standalone")]
use constellation_standalone::StandaloneService;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjSender;
use constellation_streams::threads::poll::MsgsSetNotify;
use log::debug;
use log::error;
use log::info;
use log::trace;
use log::warn;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::component::HashClientComponentTypes;
#[cfg(feature = "standalone")]
use crate::component::unicast::HashUnicastClientComponent;
use crate::component::unicast::HashUnicastClientComponentCleanup;
use crate::component::unicast::HashUnicastClientComponentTypes;
#[cfg(feature = "standalone")]
use crate::component::unicast::SimpleHashUnicastClientComponentTypes;
#[cfg(feature = "standalone")]
use crate::component::unicast::UnicastClientParam;
#[cfg(feature = "standalone")]
use crate::config::ProcessorClientConfig;
use crate::config::ProcessorConfig;
use crate::session::ClientSessionCleanup;
use crate::session::UnicastClientSession;
#[cfg(feature = "standalone")]
use crate::standalone::StandaloneCtx;

pub trait Processor<RoundID, Req, Eff, Res, Err>: Create
where
    RoundID: Clone + From<u128> + Into<u128> {
    const SERVICE_ID: &str;
    const SERVICE_NAME: &str;
    #[cfg(feature = "standalone")]
    const CONFIG_FILES: &[&str];
    #[cfg(feature = "standalone")]
    const DEFAULT_PIDFILE: &str;

    fn run_uncommitted(
        &mut self,
        req: Req,
        when: Option<XactLinPoint<RoundID>>
    ) -> Result<Res, ProcessorRunError<Err>>;

    fn run_committed(
        &mut self,
        req: Req,
        effects: Option<XactCommittedEffects<Eff>>
    ) -> Result<Res, ProcessorRunError<Err>>;
}

pub struct ProcessorSession<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes {
    types: PhantomData<Types>,
    proc: PhantomData<Proc>
}

pub struct ProcessorNotifyState<RoundID, H, Res, Err>
where
    RoundID: Clone + From<u128> + Into<u128>,
    H: HashID {
    msg: XactNotify<RoundID, H, Res, Err>,
    nretries: usize,
    when: Instant
}

pub struct ProcessorState<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes {
    pending: Mutex<
        HashMap<
            Types::HashID,
            ProcessorNotifyState<
                Types::RoundID,
                Types::HashID,
                Types::Result,
                Types::Error
            >
        >
    >,
    proc: Mutex<Proc>,
    notify: Mutex<Option<Notify>>,
    resubmit: Retry
}

#[derive(Clone)]
pub struct ProcessorSessionMsgs<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes {
    state: Arc<ProcessorState<Types, Proc>>,
    // XXX this is a hack; eventually, we'll send a registration
    // message of some kind.
    first: bool
}

#[derive(Clone)]
pub struct ProcessorSessionRecv<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes {
    state: Arc<ProcessorState<Types, Proc>>,
    class: Uuid,
    version: Version,
    when: XactLinPoint<Types::RoundID>
}

pub struct ProcessorParams {
    resubmit: Retry
}

pub struct ProcessorSessionCleanup;

pub type ProcessorCleanup<ClientTypes, Types> =
    HashUnicastClientComponentCleanup<ClientTypes, Types>;

pub enum ProcessorSessionError {
    SkippedIdx
}

pub enum ProcessorRunError<Err> {
    /// An error occurred during execution.
    Run(Err),
    /// The processor violated an effect constraint.
    EffectViolation
}

impl<RoundID, H, Res, Err> ProcessorNotifyState<RoundID, H, Res, Err>
where
    RoundID: Clone + From<u128> + Into<u128>,
    H: Clone + HashID,
    Res: Clone,
    Err: Clone
{
    fn get_notify(
        &mut self,
        resubmit: &Retry,
        now: Instant
    ) -> (Option<XactNotify<RoundID, H, Res, Err>>, Instant) {
        if self.when <= now {
            let duration = resubmit.retry_delay(self.nretries);
            let when = now + duration;

            self.nretries += 1;
            self.when = when;

            (Some(self.msg.clone()), when)
        } else {
            (None, self.when)
        }
    }
}

impl<Types, Proc> ProcessorState<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes
{
    fn get_notifies(
        &self
    ) -> Result<
        (
            Vec<
                XactNotify<
                    Types::RoundID,
                    Types::HashID,
                    Types::Result,
                    Types::Error
                >
            >,
            Option<Instant>
        ),
        MutexPoison
    > {
        let mut guard = self.pending.lock().map_err(|_| MutexPoison)?;
        let mut notifies = Vec::with_capacity(guard.len());
        let now = Instant::now();
        let mut min = None;

        for elem in guard.values_mut() {
            let (notify, when) = elem.get_notify(&self.resubmit, now);

            if let Some(notify) = notify {
                notifies.push(notify)
            }

            min = Some(min.map_or(when, |curr: Instant| curr.min(when)));
        }

        Ok((notifies, min))
    }

    fn add_notify(
        &self,
        hash: Types::HashID,
        msg: XactNotify<
            Types::RoundID,
            Types::HashID,
            Types::Result,
            Types::Error
        >
    ) -> Result<(), MutexPoison> {
        let mut guard = self.pending.lock().map_err(|_| MutexPoison)?;

        if let Entry::Vacant(ent) = guard.entry(hash.clone()) {
            trace!(target: "processor-state",
                   "adding notifiction for {}",
                   hash);

            ent.insert(ProcessorNotifyState {
                when: Instant::now(),
                nretries: 0,
                msg: msg
            });
        } else {
            trace!(target: "processor-state",
                   "notifiction for {} already existed",
                   hash);
        }

        Ok(())
    }

    fn ack(
        &self,
        hash: &Types::HashID
    ) -> Result<(), MutexPoison> {
        self.pending.lock().map_err(|_| MutexPoison)?.remove(hash);

        Ok(())
    }
}

impl<Types, Proc> MsgsSetNotify for ProcessorSessionMsgs<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes
{
    type Error = MutexPoison;

    fn set_notify(
        &mut self,
        notify: Notify
    ) -> Result<(), Self::Error> {
        *self.state.notify.lock().map_err(|_| MutexPoison)? = Some(notify);

        Ok(())
    }
}

impl<Types, Proc>
    LargeObjMsgs<
        Types::Hash,
        XactHashBatch<
            Types::RoundID,
            Types::HashID,
            Types::Seal,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >
    > for ProcessorSessionMsgs<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes
{
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<Encode>>
    where
        Encode: Debug + Display + ScopedError;

    fn add_msgs<Enc, F>(
        &mut self,
        sender: &mut LargeObjSender<
            Types::Hash,
            XactHashBatch<
                Types::RoundID,
                Types::HashID,
                Types::Seal,
                Types::Payload,
                Types::Effect,
                Types::Result,
                Types::Error
            >,
            Enc,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<Enc::EncodeError>>
    where
        Enc: Clone
            + Create
            + Encoder<
                XactHashBatch<
                    Types::RoundID,
                    Types::HashID,
                    Types::Seal,
                    Types::Payload,
                    Types::Effect,
                    Types::Result,
                    Types::Error
                >
            >,
        Enc::Config: Default,
        F: Frags {
        let (notifies, when) = self.state.get_notifies()?;

        if self.first || notifies.is_empty() {
            debug!(target: "processor-session-msgs",
                   "sending {} notifies",
                   notifies.len());

            let batch = XactHashBatch::new(vec![], vec![], notifies);

            sender
                .add_outbound(&batch)
                .map_err(|err| WithMutexPoison::Inner { err: err })?;
            self.first = false;
        }

        Ok(when)
    }
}

impl<Types, Proc> ProcessorSessionRecv<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes
{
    fn run_uncommitted(
        &mut self,
        req: XactSealed<
            Types::Seal,
            XactUncommittedHashReq<
                Types::RoundID,
                Types::HashID,
                Types::Payload,
                Types::Effect
            >
        >
    ) -> Result<
        XactNotify<Types::RoundID, Types::HashID, Types::Result, Types::Error>,
        MutexPoison
    > {
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
                XactEffects::HardNone { when } => {
                    debug!(target: "processor-recv",
                           "transaction reports hard no effects");

                    self.state
                        .proc
                        .lock()
                        .map_err(|_| MutexPoison)?
                        .run_uncommitted(payload, when)
                        .map_err(|err| match err {
                            ProcessorRunError::Run(err) => {
                                XactError::Error { err: err }
                            }
                            ProcessorRunError::EffectViolation => {
                                XactError::EffectViolation
                            }
                        })
                }
                XactEffects::SoftNone => {
                    debug!(target: "processor-recv",
                           "transaction reports soft no effects");

                    self.state
                        .proc
                        .lock()
                        .map_err(|_| MutexPoison)?
                        .run_uncommitted(payload, None)
                        .map_err(|err| match err {
                            ProcessorRunError::Run(err) => {
                                XactError::Error { err: err }
                            }
                            ProcessorRunError::EffectViolation => {
                                XactError::EffectViolation
                            }
                        })
                }
            }
        };

        match res {
            Ok(res) => {
                let state = XactNotifyState::Success {
                    result: Some(res),
                    when: self.when.clone()
                };

                Ok(XactNotify::new(hash, state))
            }
            Err(err) => {
                let state = XactNotifyState::Error { error: Some(err) };

                Ok(XactNotify::new(hash, state))
            }
        }
    }

    fn run_committed(
        &mut self,
        hash: Types::HashID,
        round: Types::RoundID,
        req: XactCommittedReq<Types::Payload, Types::Effect>
    ) -> Result<
        XactNotify<Types::RoundID, Types::HashID, Types::Result, Types::Error>,
        MutexPoison
    > {
        let (class, version, instance, idx, effects, payload) = req.take();

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
            self.state
                .proc
                .lock()
                .map_err(|_| MutexPoison)?
                .run_committed(payload, effects)
                .map_err(|err| match err {
                    ProcessorRunError::Run(err) => {
                        XactError::Error { err: err }
                    }
                    ProcessorRunError::EffectViolation => {
                        XactError::EffectViolation
                    }
                })
        };

        match res {
            Ok(res) => {
                let when = XactLinPoint::new(round, idx);
                let state = XactNotifyState::Success {
                    result: Some(res),
                    when: when.clone()
                };

                self.when = when;

                Ok(XactNotify::new(hash, state))
            }
            Err(err) => {
                let state = XactNotifyState::Error { error: Some(err) };

                Ok(XactNotify::new(hash, state))
            }
        }
    }

    fn process_uncommitted_reqs(
        &mut self,
        reqs: Vec<
            XactSealed<
                Types::Seal,
                XactUncommittedHashReq<
                    Types::RoundID,
                    Types::HashID,
                    Types::Payload,
                    Types::Effect
                >
            >
        >
    ) -> Result<bool, MutexPoison> {
        let mut notify = false;

        info!(target: "processor-recv",
              "processing uncommitted transactions");

        for req in reqs {
            let res = self.run_uncommitted(req)?;
            let hash = res.hash().clone();

            self.state.add_notify(hash, res)?;
            notify = true;
        }

        Ok(notify)
    }

    fn process_committed_rounds(
        &mut self,
        rounds: Vec<
            XactCommittedRound<
                Types::RoundID,
                Types::HashID,
                Types::Seal,
                Types::Payload,
                Types::Effect
            >
        >
    ) -> Result<
        bool,
        WithMutexPoison<
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
            >
        >
    > {
        let codec_config = XactBatchCodecConfig::default();
        let mut codec: XactHashBatchCodec<
            Types::RoundID,
            Types::Hash,
            _,
            _,
            _,
            _,
            _,
            Types::SealCodec,
            Types::PayloadCodec,
            Types::EffectCodec,
            Types::ResultCodec,
            Types::ErrorCodec
        > = XactHashBatchCodec::create(codec_config)
            .map_err(|err| WithMutexPoison::Inner { err: err })?;
        let mut notify = false;

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
                            let idx = req.idx();

                            if hashes.len() <= idx && hashes[idx] == hash {
                                let res = self
                                    .run_committed(hash, id.clone(), req)
                                    .map_err(|_| {
                                        WithMutexPoison::MutexPoison
                                    })?;
                                let hash = res.hash().clone();

                                self.state.add_notify(hash, res)?;
                            } else {
                                let state = XactNotifyState::Error {
                                    error: Some(XactError::HashMismatch)
                                };
                                let res = XactNotify::new(hash, state);
                                let hash = res.hash().clone();

                                self.state.add_notify(hash, res)?;
                                notify = true;
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
                            let res = self
                                .run_committed(hash, id.clone(), req)
                                .map_err(|_| WithMutexPoison::MutexPoison)?;
                            let hash = res.hash().clone();

                            self.state.add_notify(hash, res)?;
                            notify = true;
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

        Ok(notify)
    }
}

impl<Types, Proc> AuthNMsgRecv<Types::MsgPrin, Types::AuthNMsg>
    for ProcessorSessionRecv<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes
{
    type RecvError = WithMutexPoison<Error>;

    fn recv_auth_msg(
        &mut self,
        msg: Types::AuthNMsg
    ) -> Result<(), Self::RecvError> {
        let (prin, msg) = msg.take();
        let (committed, uncommitted, notifies) = msg.take();
        let mut notify = false;

        info!(target: "processor-recv",
              "received batch from {}",
              prin);

        notify |= self
            .process_uncommitted_reqs(uncommitted)
            .map_err(|_| WithMutexPoison::MutexPoison)?;
        notify |= self
            .process_committed_rounds(committed)
            .map_err(|_| WithMutexPoison::MutexPoison)?;

        for notify in notifies {
            warn!(target: "processor-recv",
                  "discarded notification for {}: {}",
                  notify.hash(), notify.state())
        }

        if notify {
            if let Some(notify) = &mut *self
                .state
                .notify
                .lock()
                .map_err(|_| WithMutexPoison::MutexPoison)?
            {
                trace!(target: "processor-recv",
                       "notifying message thread");

                notify
                    .notify()
                    .map_err(|err| WithMutexPoison::Inner { err: err })?;
            } else {
                error!(target: "processor-recv",
                       "notify is not set");
            }
        }

        Ok(())
    }
}

impl ClientSessionCleanup for ProcessorSessionCleanup {
    fn cleanup(self) {}
}

impl<Types, Proc>
    UnicastClientSession<
        ProcessorSessionRecv<Types, Proc>,
        ProcessorSessionMsgs<Types, Proc>
    > for ProcessorSession<Types, Proc>
where
    Proc: Processor<
            Types::RoundID,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >,
    Types: HashClientComponentTypes
{
    type Cleanup = ProcessorSessionCleanup;
    type Config = ProcessorConfig<Proc::Config>;
    type CreateError = Proc::CreateError;
    type Param = ();
    type StartError = MutexPoison;

    fn create(
        config: Self::Config,
        _param: ()
    ) -> Result<
        (
            Self,
            ProcessorSessionRecv<Types, Proc>,
            ProcessorSessionMsgs<Types, Proc>
        ),
        Self::CreateError
    > {
        let (proc_config, resubmit) = config.take();
        let class =
            Uuid::new_v5(&Uuid::NAMESPACE_DNS, Proc::SERVICE_ID.as_bytes());
        let version = Version::new(0, 0, 0);
        let proc = Proc::create(proc_config)?;
        let state = ProcessorState {
            pending: Mutex::new(HashMap::new()),
            resubmit: resubmit,
            proc: Mutex::new(proc),
            notify: Mutex::new(None)
        };
        let state = Arc::new(state);
        let msgs = ProcessorSessionMsgs {
            state: state.clone(),
            first: true
        };
        let recv = ProcessorSessionRecv {
            state: state,
            class: class,
            version: version,
            when: XactLinPoint::new(Types::RoundID::from(0), 0)
        };
        let out = ProcessorSession {
            types: PhantomData,
            proc: PhantomData
        };

        Ok((out, recv, msgs))
    }

    fn start(self) -> Result<Self::Cleanup, Self::StartError> {
        Ok(ProcessorSessionCleanup)
    }
}

#[cfg(feature = "standalone")]
pub struct StandaloneProcessorCreateCleanup {
    caches: Option<(ShutdownFlag, JoinHandle<()>)>
}

#[cfg(feature = "standalone")]
pub struct StandaloneProcessorRunCleanup<ClientTypes, Types>
where
    ClientTypes: HashClientComponentTypes,
    Types: HashUnicastClientComponentTypes<ClientTypes> {
    component: HashUnicastClientComponentCleanup<ClientTypes, Types>
}

#[cfg(feature = "standalone")]
pub struct StandaloneProcessor<ClientTypes, Types, Proc>
where
    ClientTypes: HashClientComponentTypes,
    Types: HashUnicastClientComponentTypes<
            ClientTypes,
            Session = ProcessorSession<ClientTypes, Proc>,
            SessionParam = (),
            SessionConfig = ProcessorConfig<Proc::Config>,
            Ctx = StandaloneCtx
        >,
    Proc: Processor<
            ClientTypes::RoundID,
            ClientTypes::Payload,
            ClientTypes::Effect,
            ClientTypes::Result,
            ClientTypes::Error
        > {
    proc: PhantomData<Proc>,
    component: HashUnicastClientComponent<ClientTypes, Types>
}

#[cfg(feature = "standalone")]
pub type SimpleStandaloneProcessor<ClientTypes, BusTypes, Proc> =
    StandaloneProcessor<
        ClientTypes,
        SimpleHashUnicastClientComponentTypes<
            ClientTypes,
            BusTypes,
            ProcessorSession<ClientTypes, Proc>,
            StandaloneCtx
        >,
        Proc
    >;

#[cfg(feature = "standalone")]
impl<ClientTypes, Types> StandaloneProcessorRunCleanup<ClientTypes, Types>
where
    ClientTypes: 'static + HashClientComponentTypes,
    Types: 'static + HashUnicastClientComponentTypes<ClientTypes>
{
    fn cleanup(self) {
        self.component.cleanup()
    }
}

#[cfg(feature = "standalone")]
impl<ClientTypes, Types, Proc> Standalone
    for StandaloneProcessor<ClientTypes, Types, Proc>
where
    ClientTypes: HashClientComponentTypes,
    Types: HashUnicastClientComponentTypes<
            ClientTypes,
            Session = ProcessorSession<ClientTypes, Proc>,
            SessionParam = (),
            SessionConfig = ProcessorConfig<Proc::Config>,
            Ctx = StandaloneCtx
        >,
    Proc: Processor<
            ClientTypes::RoundID,
            ClientTypes::Payload,
            ClientTypes::Effect,
            ClientTypes::Result,
            ClientTypes::Error
        >,
    Proc::Config: for<'a> Deserialize<'a> + Serialize
{
    type Config = ProcessorClientConfig<
        Proc::Config,
        ClientTypes::SealCodecConfig,
        ClientTypes::PayloadCodecConfig,
        ClientTypes::EffectCodecConfig,
        ClientTypes::ResultCodecConfig,
        ClientTypes::ErrorCodecConfig,
        ClientTypes::HashConfig,
        Types::SessionAuthConfig,
        Types::LargeObjMsgAuthConfig,
        Types::UnixConfig,
        Types::UDPConfig,
        Types::IDsConfig,
        Types::ResolverConfig,
        Types::EpochsConfig,
        Types::MsgAuthConfig
    >;
    type CreateCleanup = StandaloneProcessorCreateCleanup;

    const CONFIG_FILES: &[&str] = Proc::CONFIG_FILES;
    const NAME: &str = Proc::SERVICE_NAME;
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn create(
        _args: ArgMatches,
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let (name_caches_config, client_config) = config.take();
        let (caches, caches_join, shutdown) = ThreadedNSNameCaches::create(
            name_caches_config
        )
        .map_err(|err| {
            error!(target: "processor-standalone",
                       "error creating name caches: {}",
                       err);

            StandaloneProcessorCreateCleanup { caches: None }
        })?;
        let ctx = StandaloneCtx::new(caches);
        let cleanup = StandaloneProcessorCreateCleanup {
            caches: Some((shutdown.clone(), caches_join))
        };
        let param = UnicastClientParam::new((), ctx);

        match HashUnicastClientComponent::create(client_config, param) {
            Ok(component) => {
                let out = StandaloneProcessor {
                    proc: PhantomData,
                    component: component
                };

                Ok((out, cleanup))
            }
            Err(err) => {
                error!(target: "processor-standalone",
                       "error creating multicast client: {}",
                       err);

                Err(cleanup)
            }
        }
    }
}

#[cfg(feature = "standalone")]
impl<ClientTypes, Types, Proc> StandaloneService
    for StandaloneProcessor<ClientTypes, Types, Proc>
where
    ClientTypes: 'static + HashClientComponentTypes,
    Types: 'static
        + HashUnicastClientComponentTypes<
            ClientTypes,
            Session = ProcessorSession<ClientTypes, Proc>,
            SessionParam = (),
            SessionConfig = ProcessorConfig<Proc::Config>,
            Ctx = StandaloneCtx
        >,
    Proc: Processor<
            ClientTypes::RoundID,
            ClientTypes::Payload,
            ClientTypes::Effect,
            ClientTypes::Result,
            ClientTypes::Error
        >,
    Proc::Config: for<'a> Deserialize<'a> + Serialize
{
    type RunCleanup = StandaloneProcessorRunCleanup<ClientTypes, Types>;
    type RunErrorCleanup = ();

    const DEFAULT_PIDFILE: &str = Proc::DEFAULT_PIDFILE;

    fn run(
        self,
        _rundir: PathBuf
    ) -> Result<Self::RunCleanup, Self::RunErrorCleanup> {
        match self.component.start() {
            Ok(cleanup) => {
                let cleanup =
                    StandaloneProcessorRunCleanup { component: cleanup };

                Ok(cleanup)
            }
            Err(err) => {
                error!(target: "processor",
                       "{}", err);

                Err(())
            }
        }
    }

    fn shutdown(
        create_cleanup: Self::CreateCleanup,
        run_cleanup: Option<Self::RunCleanup>
    ) {
        let StandaloneProcessorCreateCleanup { caches } = create_cleanup;

        debug!(target: "processor",
               "cleaning up processor");

        if let Some(cleanup) = run_cleanup {
            debug!(target: "processor",
               "cleaning up runtime");

            cleanup.cleanup();
        }

        debug!(target: "processor",
               "cleaning up caches");

        if let Some((mut shutdown, join)) = caches {
            if let Err(err) = shutdown.set() {
                error!(target: "one-shot",
                       "error setting shutdown flag: {}",
                       err)
            } else if join.join().is_err() {
                error!(target: "one-shot",
                       "error shutting down name chache threads")
            }
        }
    }

    fn shutdown_err(
        create_cleanup: Self::CreateCleanup,
        _run_cleanup: Self::RunErrorCleanup
    ) {
        let StandaloneProcessorCreateCleanup { caches } = create_cleanup;

        if let Some((mut shutdown, join)) = caches {
            if let Err(err) = shutdown.set() {
                error!(target: "one-shot",
                       "error setting shutdown flag: {}",
                       err)
            } else if join.join().is_err() {
                error!(target: "one-shot",
                       "error shutting down name chache threads")
            }
        }
    }
}
