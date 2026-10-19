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
use std::fmt::Error;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(feature = "standalone")]
use std::thread::JoinHandle;
use std::time::Instant;

#[cfg(feature = "standalone")]
use clap::ArgMatches;
#[cfg(feature = "standalone")]
use clap::Command;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNedDestruct;
#[cfg(feature = "standalone")]
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
#[cfg(feature = "standalone")]
use constellation_common::config::CreateWithParam;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::retry::Retry;
#[cfg(feature = "standalone")]
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
#[cfg(feature = "standalone")]
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
#[cfg(feature = "standalone")]
use constellation_common::version::VersionSuffix;
use constellation_component_common::xact::XactBatch;
use constellation_component_common::xact::XactEffects;
use constellation_component_common::xact::XactNotifyState;
use constellation_component_common::xact::XactSealed;
use constellation_component_common::xact::XactUncommittedReq;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
#[cfg(feature = "standalone")]
use constellation_standalone::StandaloneApp;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjSender;
use constellation_streams::multicast::MulticastStreamIdx;
use constellation_streams::threads::poll::MsgsSetNotify;
use log::debug;
#[cfg(feature = "standalone")]
use log::error;
use log::info;
use log::warn;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::component::ClientComponentTypes;
#[cfg(feature = "standalone")]
use crate::component::multicast::MulticastClientComponent;
use crate::component::multicast::MulticastClientComponentTypes;
#[cfg(feature = "standalone")]
use crate::component::multicast::MulticastClientParam;
#[cfg(feature = "standalone")]
use crate::component::multicast::SimpleMulticastClientComponentTypes;
#[cfg(feature = "standalone")]
use crate::config::OneShotClientConfig;
use crate::config::OneShotConfig;
use crate::session::ClientSessionCleanup;
use crate::session::MulticastClientSession;
#[cfg(feature = "standalone")]
use crate::standalone::StandaloneCtx;

pub trait OneShot<RoundID, Req, Eff, Seal>: Create
where
    RoundID: Clone + From<u128> + Into<u128> {
    const NAME: &str;
    #[cfg(feature = "standalone")]
    const CONFIG_FILES: &[&str];

    type Param;
    type GetArgError: Debug + Display;
    type CreateSealError: Debug + Display;

    fn get_msg(
        &self,
        param: Self::Param
    ) -> (Uuid, Version, u64, Req, XactEffects<RoundID, Eff>);

    fn create_seal(
        &self,
        req: &XactUncommittedReq<RoundID, Req, Eff>
    ) -> Result<Seal, Self::CreateSealError>;

    #[cfg(feature = "standalone")]
    fn get_arg(args: ArgMatches) -> Result<Self::Param, Self::GetArgError>;

    #[cfg(feature = "standalone")]
    fn cmdargs(cmd: Command) -> Command {
        cmd
    }
}

struct OneShotMsg<RoundID, Payload, Effect, Seal>
where
    RoundID: Clone + From<u128> + Into<u128> {
    msg: XactSealed<Seal, XactUncommittedReq<RoundID, Payload, Effect>>,
    nretries: usize,
    when: Instant
}

pub struct OneShotSession<Types, App>
where
    Types: ClientComponentTypes,
    App: OneShot<Types::RoundID, Types::Payload, Types::Effect, Types::Seal> {
    types: PhantomData<Types>,
    os: PhantomData<App>
}

pub struct OneShotState<Types>
where
    Types: ClientComponentTypes {
    msg: Mutex<
        Option<
            OneShotMsg<
                Types::RoundID,
                Types::Payload,
                Types::Effect,
                Types::Seal
            >
        >
    >,
    notify: Mutex<Option<Notify>>,
    resubmit: Retry
}

#[derive(Clone)]
pub struct OneShotSessionMsgs<Types>
where
    Types: ClientComponentTypes {
    state: Arc<OneShotState<Types>>
}

#[derive(Clone)]
pub struct OneShotSessionRecv<Types>
where
    Types: ClientComponentTypes {
    state: Arc<OneShotState<Types>>
}

pub struct OneShotSessionCleanup;

#[derive(Debug)]
pub enum OneShotError<Create, Seal> {
    Create { err: Create },
    Seal { err: Seal }
}

impl<Types> OneShotState<Types>
where
    Types: ClientComponentTypes
{
    fn get_batch(
        &self
    ) -> Result<
        (
            Option<
                XactBatch<
                    Types::RoundID,
                    Types::HashID,
                    Types::Seal,
                    Types::Payload,
                    Types::Effect,
                    Types::Result,
                    Types::Error
                >
            >,
            Option<Instant>
        ),
        MutexPoison
    > {
        if let Some(msg) = &mut *self.msg.lock().map_err(|_| MutexPoison)? {
            let now = Instant::now();

            if now >= msg.when {
                debug!(target: "one-shot-msgs",
                       "generating outgoing batch");

                let sealed = msg.msg.clone();
                let batch = XactBatch::new(vec![], vec![sealed], vec![]);
                let duration = self.resubmit.retry_delay(msg.nretries);
                let when = now + duration;

                msg.when = when;
                msg.nretries += 1;

                Ok((Some(batch), Some(when)))
            } else {
                Ok((None, Some(msg.when)))
            }
        } else {
            Ok((None, None))
        }
    }

    fn notify(&self) -> Result<Option<Instant>, MutexPoison> {
        if let Some(msg) = &mut *self.msg.lock().map_err(|_| MutexPoison)? {
            let now = Instant::now();
            let duration = self.resubmit.retry_delay(msg.nretries);
            let when = now + duration;

            msg.when = when;

            Ok(Some(when))
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn complete(&self) -> Result<(), MutexPoison> {
        *self.msg.lock().map_err(|_| MutexPoison)? = None;

        Ok(())
    }
}

impl<Types> MsgsSetNotify for OneShotSessionMsgs<Types>
where
    Types: ClientComponentTypes
{
    type Error = MutexPoison;

    #[inline]
    fn set_notify(
        &mut self,
        notify: Notify
    ) -> Result<(), Self::Error> {
        *self.state.notify.lock().map_err(|_| MutexPoison)? = Some(notify);

        Ok(())
    }
}

impl<Types>
    LargeObjMsgs<
        Types::Hash,
        XactBatch<
            Types::RoundID,
            Types::HashID,
            Types::Seal,
            Types::Payload,
            Types::Effect,
            Types::Result,
            Types::Error
        >
    > for OneShotSessionMsgs<Types>
where
    Types: ClientComponentTypes
{
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<Encode>>
    where
        Encode: Debug + Display + ScopedError;

    fn add_msgs<Enc, F>(
        &mut self,
        sender: &mut LargeObjSender<
            Types::Hash,
            XactBatch<
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
                XactBatch<
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
        let (batch, when) = self
            .state
            .get_batch()
            .map_err(|_| WithMutexPoison::MutexPoison)?;

        if let Some(batch) = batch {
            sender
                .add_outbound(&batch)
                .map_err(|err| WithMutexPoison::Inner { err: err })?;
        }

        Ok(when)
    }
}

impl<Types> AuthNMsgRecv<Types::MsgPrin, Types::AuthNMsg>
    for OneShotSessionRecv<Types>
where
    Types: ClientComponentTypes
{
    type RecvError = MutexPoison;

    fn recv_auth_msg(
        &mut self,
        msg: Types::AuthNMsg
    ) -> Result<(), Self::RecvError> {
        let (_, msg) = msg.take();

        for _ in msg.uncommitted() {
            warn!(target: "one-shot-recv",
                  "discarding uncommitted request")
        }

        for _ in msg.committed() {
            warn!(target: "one-shot-recv",
                  "discarding committed round")
        }

        for notify in msg.notifies() {
            match notify.state() {
                XactNotifyState::Success { .. } |
                XactNotifyState::Error { .. } => {
                    self.state.complete()?;
                }
                _ => {
                    self.state.notify()?;
                }
            }

            info!(target: "one-shot-recv",
                  "notification for {}: {}",
                  notify.hash(), notify.state())
        }

        Ok(())
    }
}

impl ClientSessionCleanup for OneShotSessionCleanup {
    fn cleanup(self) {}
}

impl<Types, Prin, App>
    MulticastClientSession<
        Prin,
        OneShotSessionRecv<Types>,
        OneShotSessionMsgs<Types>
    > for OneShotSession<Types, App>
where
    Types: ClientComponentTypes,
    App: OneShot<Types::RoundID, Types::Payload, Types::Effect, Types::Seal>
{
    type Cleanup = OneShotSessionCleanup;
    type Config = OneShotConfig<App::Config>;
    type CreateError = OneShotError<App::CreateError, App::CreateSealError>;
    type Param = App::Param;
    type StartError = MutexPoison;

    fn create(
        config: Self::Config,
        param: App::Param
    ) -> Result<
        (Self, OneShotSessionRecv<Types>, OneShotSessionMsgs<Types>),
        Self::CreateError
    > {
        let (app_config, resubmit) = config.take();
        let os = App::create(app_config)
            .map_err(|err| OneShotError::Create { err: err })?;
        let (uuid, version, instance, payload, effects) = os.get_msg(param);
        let req =
            XactUncommittedReq::new(uuid, version, instance, payload, effects);
        let seal = os
            .create_seal(&req)
            .map_err(|err| OneShotError::Seal { err: err })?;
        let msg = OneShotMsg {
            msg: XactSealed::new(seal, req),
            nretries: 0,
            when: Instant::now()
        };
        let state = OneShotState {
            resubmit: resubmit,
            notify: Mutex::new(None),
            msg: Mutex::new(Some(msg))
        };
        let state = Arc::new(state);
        let msgs = OneShotSessionMsgs {
            state: state.clone()
        };
        let recv = OneShotSessionRecv { state: state };
        let out = OneShotSession {
            types: PhantomData,
            os: PhantomData
        };

        Ok((out, recv, msgs))
    }

    fn start<I>(
        self,
        _parties: I
    ) -> Result<Self::Cleanup, Self::StartError>
    where
        I: Iterator<Item = (MulticastStreamIdx, Prin)> {
        Ok(OneShotSessionCleanup)
    }
}

impl<Create, Seal> Display for OneShotError<Create, Seal>
where
    Create: Display,
    Seal: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            OneShotError::Create { err } => err.fmt(f),
            OneShotError::Seal { err } => err.fmt(f)
        }
    }
}

#[cfg(feature = "standalone")]
pub struct StandaloneOneShotCreateCleanup {
    caches: Option<(ShutdownFlag, JoinHandle<()>)>
}

#[cfg(feature = "standalone")]
pub struct StandaloneOneShotRunCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

#[cfg(feature = "standalone")]
pub struct StandaloneOneShot<ClientTypes, Types, App>
where
    ClientTypes: ClientComponentTypes,
    Types: MulticastClientComponentTypes<ClientTypes>,
    App: OneShot<
            ClientTypes::RoundID,
            ClientTypes::Payload,
            ClientTypes::Effect,
            ClientTypes::Seal
        > {
    app: PhantomData<App>,
    component: MulticastClientComponent<ClientTypes, Types>
}

#[cfg(feature = "standalone")]
pub type SimpleStandaloneOneShot<ClientTypes, BusTypes, App> =
    StandaloneOneShot<
        ClientTypes,
        SimpleMulticastClientComponentTypes<
            ClientTypes,
            BusTypes,
            OneShotSession<ClientTypes, App>,
            StandaloneCtx
        >,
        App
    >;

#[cfg(feature = "standalone")]
type StandaloneMulticastClientComponentTypes<ClientTypes, BusTypes, App> =
    SimpleMulticastClientComponentTypes<
        ClientTypes,
        BusTypes,
        OneShotSession<ClientTypes, App>,
        StandaloneCtx
    >;

#[cfg(feature = "standalone")]
impl<ClientTypes, Types, App> Standalone
    for StandaloneOneShot<ClientTypes, Types, App>
where
    ClientTypes: ClientComponentTypes,
    Types: MulticastClientComponentTypes<
            ClientTypes,
            Session = OneShotSession<ClientTypes, App>,
            SessionParam = App::Param,
            SessionConfig = OneShotConfig<App::Config>,
            Ctx = StandaloneCtx
        >,
    App: OneShot<
            ClientTypes::RoundID,
            ClientTypes::Payload,
            ClientTypes::Effect,
            ClientTypes::Seal
        >,
    App::Config: for<'a> Deserialize<'a> + Serialize
{
    type Config = OneShotClientConfig<
        Types::SessionPrin,
        App::Config,
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
    type CreateCleanup = StandaloneOneShotCreateCleanup;

    const CONFIG_FILES: &[&str] = App::CONFIG_FILES;
    const NAME: &str = App::NAME;
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    #[inline]
    fn cmdargs(cmd: Command) -> Command {
        App::cmdargs(cmd)
    }

    fn create(
        args: ArgMatches,
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let (name_caches_config, client_config) = config.take();
        let (caches, caches_join, shutdown) = ThreadedNSNameCaches::create(
            name_caches_config
        )
        .map_err(|err| {
            error!(target: "one-shot-standalone",
                       "error creating name caches: {}",
                       err);

            StandaloneOneShotCreateCleanup { caches: None }
        })?;
        let ctx = StandaloneCtx::new(caches);
        let cleanup = StandaloneOneShotCreateCleanup {
            caches: Some((shutdown.clone(), caches_join))
        };

        match App::get_arg(args) {
            Ok(arg) => {
                let param = MulticastClientParam::new(arg, ctx);

                match MulticastClientComponent::create(client_config, param) {
                    Ok(component) => {
                        let out = StandaloneOneShot {
                            app: PhantomData,
                            component: component
                        };

                        Ok((out, cleanup))
                    }
                    Err(err) => {
                        error!(target: "one-shot-standalone",
                               "error creating multicast client: {}",
                               err);

                        Err(cleanup)
                    }
                }
            }
            Err(err) => {
                error!(target: "one-shot-recv",
                       "error parsing application args: {}",
                       err);

                Err(cleanup)
            }
        }
    }
}

#[cfg(feature = "standalone")]
impl<ClientTypes, Types, App> StandaloneApp
    for StandaloneOneShot<ClientTypes, Types, App>
where
    ClientTypes: 'static + ClientComponentTypes,
    Types: 'static
        + MulticastClientComponentTypes<
            ClientTypes,
            Session = OneShotSession<ClientTypes, App>,
            SessionParam = App::Param,
            SessionConfig = OneShotConfig<App::Config>,
            Ctx = StandaloneCtx
        >,
    App: OneShot<
            ClientTypes::RoundID,
            ClientTypes::Payload,
            ClientTypes::Effect,
            ClientTypes::Seal
        >,
    App::Config: for<'a> Deserialize<'a> + Serialize
{
    type RunErrorCleanup = ();

    fn run(self) -> Result<(), Self::RunErrorCleanup> {
        info!(target: "one-shot",
              "starting one-shot component");

        let cleanup = self.component.start().map_err(|err| {
            error!(target: "one-shot",
                       "error starting component: {}",
                       err);
        })?;

        cleanup.cleanup();

        Ok(())
    }

    fn cleanup(_create: Self::CreateCleanup) {}

    fn cleanup_err(
        create: Self::CreateCleanup,
        _run: Self::RunErrorCleanup
    ) {
        let StandaloneOneShotCreateCleanup { caches } = create;

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
