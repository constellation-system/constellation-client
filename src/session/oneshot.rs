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
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNedDestruct;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::retry::Retry;
use constellation_common::sync::Notify;
use constellation_common::version::Version;
use constellation_component_common::xact::XactBatch;
use constellation_component_common::xact::XactEffects;
use constellation_component_common::xact::XactNotifyState;
use constellation_component_common::xact::XactSealed;
use constellation_component_common::xact::XactUncommittedReq;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjSender;
use constellation_streams::multicast::MulticastStreamIdx;
use constellation_streams::threads::poll::MsgsSetNotify;
use log::debug;
use log::info;
use log::warn;
use uuid::Uuid;

use crate::component::ClientComponentTypes;
use crate::component::multicast::MulticastClientComponentTypes;
use crate::session::MulticastClientSession;
use crate::session::ClientSessionCleanup;

pub trait OneShot<RoundID, Req, Eff, Seal>: Create
where RoundID: Clone + From<u128> + Into<u128> {
    const SERVICE_NAME: &str;

    type Arg;
    type CreateSealError: Debug + Display;

    fn get_msg(
        &self,
        arg: Self::Arg
    ) -> (Uuid, Version, u64, Req, XactEffects<RoundID, Eff>);

    fn create_seal(
        &self,
        req: &XactUncommittedReq<RoundID, Req, Eff>
    ) -> Result<Seal, Self::CreateSealError>;
}

struct OneShotMsg<RoundID, Payload, Effect, Seal>
where RoundID: Clone + From<u128> + Into<u128> {
    msg: XactSealed<
        Seal,
        XactUncommittedReq<RoundID, Payload, Effect>,
    >,
    nretries: usize,
    when: Instant
}

pub struct OneShotSession<Types, OS>
where Types: MulticastClientComponentTypes,
      OS: OneShot<Types::RoundID, Types::Payload, Types::Effect, Types::Seal> {
    types: PhantomData<Types>,
    os: PhantomData<OS>
}

pub struct OneShotState<Types>
where Types: ClientComponentTypes {
    msg: Mutex<Option<OneShotMsg<Types::RoundID, Types::Payload,
                                 Types::Effect, Types::Seal>>>,
    notify: Mutex<Option<Notify>>,
    resubmit: Retry,
}

#[derive(Clone)]
pub struct OneShotSessionMsgs<Types>
where Types: ClientComponentTypes {
    state: Arc<OneShotState<Types>>,
}

#[derive(Clone)]
pub struct OneShotSessionRecv<Types>
where Types: ClientComponentTypes {
    state: Arc<OneShotState<Types>>,
}

pub struct OneShotArgs<Arg> {
    arg: Arg,
    resubmit: Retry,
}

pub struct OneShotSessionCleanup;

#[derive(Debug)]
pub enum OneShotError<Create, Seal> {
    Create {
        err: Create
    },
    Seal {
        err: Seal
    }
}

impl<Types> OneShotState<Types>
where Types: ClientComponentTypes {
    fn get_batch(
        &self,
    ) -> Result<
        (Option<XactBatch<
             Types::RoundID,
             Types::HashID,
             Types::Seal,
             Types::Payload,
             Types::Effect,
             Types::Result,
             Types::Error
         >>,
         Option<Instant>),
        MutexPoison
    > {
        if let Some(msg) = &mut *self.msg.lock().map_err(|_| MutexPoison)? {
            let now = Instant::now();

            if now >= msg.when {
                debug!(target: "cmdline-msgs",
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
where Types: ClientComponentTypes {
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

impl<Types> LargeObjMsgs<
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
where Types: ClientComponentTypes {
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
        Enc: Clone + Create
            + Encoder<XactBatch<
                Types::RoundID,
                Types::HashID,
                Types::Seal,
                Types::Payload,
                Types::Effect,
                Types::Result,
                Types::Error
            >>,
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
where Types: ClientComponentTypes {
    type RecvError = MutexPoison;

    fn recv_auth_msg(
        &mut self,
        msg: Types::AuthNMsg
    ) -> Result<(), Self::RecvError> {
        let (_, msg) = msg.take();

        for _ in msg.uncommitted() {
            warn!(target: "cmdline-recv",
                  "discarding uncommitted request")
        }

        for _ in msg.committed() {
            warn!(target: "cmdline-recv",
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

            info!(target: "cmdline-recv",
                  "notification for {}: {}",
                  notify.hash(), notify.state())
        }

        Ok(())
    }
}

impl ClientSessionCleanup for OneShotSessionCleanup {
    fn cleanup(self) {}
}

impl<Types, OS> MulticastClientSession<Types::SessionPrin,
                                   OneShotSessionRecv<Types>,
                                   OneShotSessionMsgs<Types>,
                                   OneShotArgs<OS::Arg>>
    for OneShotSession<Types, OS>
where Types: MulticastClientComponentTypes,
      OS: OneShot<Types::RoundID, Types::Payload, Types::Effect, Types::Seal> {
    type Cleanup = OneShotSessionCleanup;
    type Config = OS::Config;
    type CreateError = OneShotError<OS::CreateError, OS::CreateSealError>;
    type StartError = MutexPoison;

    fn create(
        config: Self::Config,
        args: OneShotArgs<OS::Arg>
    ) -> Result<
            (Self,
             OneShotSessionRecv<Types>,
             OneShotSessionMsgs<Types>),
        Self::CreateError
    > {
        let OneShotArgs { arg, resubmit } = args;
        let os = OS::create(config)
            .map_err(|err| OneShotError::Create { err: err })?;
        let (uuid, version, instance, payload, effects) = os.get_msg(arg);
        let req = XactUncommittedReq::new(uuid, version, instance,
                                          payload, effects);
        let seal = os.create_seal(&req)
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
            state: state.clone(),
        };
        let recv = OneShotSessionRecv {
            state: state,
        };
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
        I: Iterator<Item = (MulticastStreamIdx, Types::SessionPrin)> {
        Ok(OneShotSessionCleanup)
    }
}

impl<Create, Seal> Display for OneShotError<Create, Seal>
where Create: Display,
      Seal: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            OneShotError::Create { err } => err.fmt(f),
            OneShotError::Seal { err } => err.fmt(f),
        }
    }
}
