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

use constellation_channels::config::ThreadedNSNameCachesConfig;
use constellation_common::retry::Retry;
use constellation_component_common::config::MulticastLargeObjBusConfig;
use constellation_component_common::config::UnicastLargeObjBusConfig;
use constellation_component_common::config::XactBatchCodecConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "one-shot-client")]
#[serde(rename_all = "kebab-case")]
pub struct OneShotClientConfig<
    Prin,
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
> where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default {
    /// Name cache configuration.
    #[serde(default)]
    name_caches: ThreadedNSNameCachesConfig,
    #[serde(flatten)]
    client: MulticastClientConfig<
        Prin,
        OneShotConfig<Session>,
        Seal,
        Payload,
        Effect,
        Res,
        Err,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "one-shot")]
#[serde(rename_all = "kebab-case")]
pub struct OneShotConfig<App> {
    #[serde(flatten)]
    app: App,
    #[serde(default)]
    resubmit: Retry
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "processor-client")]
#[serde(rename_all = "kebab-case")]
pub struct ProcessorClientConfig<
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
> where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default {
    /// Name cache configuration.
    #[serde(default)]
    name_caches: ThreadedNSNameCachesConfig,
    #[serde(flatten)]
    client: UnicastClientConfig<
        ProcessorConfig<Session>,
        Seal,
        Payload,
        Effect,
        Res,
        Err,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "processor")]
#[serde(rename_all = "kebab-case")]
pub struct ProcessorConfig<Proc> {
    #[serde(flatten)]
    proc: Proc,
    #[serde(default)]
    resubmit: Retry
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "multicast-client")]
#[serde(rename_all = "kebab-case")]
pub struct MulticastClientConfig<
    Prin,
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
> where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default {
    #[serde(flatten)]
    multicast: MulticastLargeObjBusConfig<
        Prin,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >,
    #[serde(flatten)]
    session: Session
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "unicast-client")]
#[serde(rename_all = "kebab-case")]
pub struct UnicastClientConfig<
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
> where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default {
    #[serde(flatten)]
    unicast: UnicastLargeObjBusConfig<
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >,
    #[serde(flatten)]
    session: Session
}

impl<
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
>
    UnicastClientConfig<
        Session,
        Seal,
        Payload,
        Effect,
        Res,
        Err,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >
where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default
{
    #[inline]
    pub fn new(
        unicast: UnicastLargeObjBusConfig<
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >,
        session: Session
    ) -> Self {
        UnicastClientConfig {
            unicast: unicast,
            session: session
        }
    }

    #[inline]
    pub fn unicast(
        &self
    ) -> &UnicastLargeObjBusConfig<
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    > {
        &self.unicast
    }

    #[inline]
    pub fn session(&self) -> &Session {
        &self.session
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        UnicastLargeObjBusConfig<
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >,
        Session
    ) {
        (self.unicast, self.session)
    }
}

impl<
    Prin,
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
>
    MulticastClientConfig<
        Prin,
        Session,
        Seal,
        Payload,
        Effect,
        Res,
        Err,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >
where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default
{
    #[inline]
    pub fn new(
        multicast: MulticastLargeObjBusConfig<
            Prin,
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >,
        session: Session
    ) -> Self {
        MulticastClientConfig {
            multicast: multicast,
            session: session
        }
    }

    #[inline]
    pub fn multicast(
        &self
    ) -> &MulticastLargeObjBusConfig<
        Prin,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    > {
        &self.multicast
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        MulticastLargeObjBusConfig<
            Prin,
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            XactBatchCodecConfig<Seal, Payload, Effect, Res, Err>,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >,
        Session
    ) {
        (self.multicast, self.session)
    }
}

impl<
    Prin,
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
>
    OneShotClientConfig<
        Prin,
        Session,
        Seal,
        Payload,
        Effect,
        Res,
        Err,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >
where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default
{
    #[inline]
    pub fn new(
        name_caches: ThreadedNSNameCachesConfig,
        client: MulticastClientConfig<
            Prin,
            OneShotConfig<Session>,
            Seal,
            Payload,
            Effect,
            Res,
            Err,
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >
    ) -> Self {
        OneShotClientConfig {
            client: client,
            name_caches: name_caches
        }
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        ThreadedNSNameCachesConfig,
        MulticastClientConfig<
            Prin,
            OneShotConfig<Session>,
            Seal,
            Payload,
            Effect,
            Res,
            Err,
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >
    ) {
        (self.name_caches, self.client)
    }
}

impl<App> OneShotConfig<App> {
    #[inline]
    pub fn new(
        app: App,
        resubmit: Retry
    ) -> Self {
        OneShotConfig {
            resubmit: resubmit,
            app: app
        }
    }

    #[inline]
    pub fn take(self) -> (App, Retry) {
        (self.app, self.resubmit)
    }
}

impl<
    Session,
    Seal,
    Payload,
    Effect,
    Res,
    Err,
    Hash,
    AuthN,
    ProtoAuthN,
    Unix,
    UDP,
    IDs,
    Resolver,
    Epochs,
    MsgAuthN
>
    ProcessorClientConfig<
        Session,
        Seal,
        Payload,
        Effect,
        Res,
        Err,
        Hash,
        AuthN,
        ProtoAuthN,
        Unix,
        UDP,
        IDs,
        Resolver,
        Epochs,
        MsgAuthN
    >
where
    Hash: Default,
    Unix: Default,
    UDP: Default,
    Seal: Default,
    Payload: Default,
    Effect: Default,
    Res: Default,
    Err: Default,
    IDs: Default,
    Epochs: Default,
    Resolver: Default
{
    #[inline]
    pub fn new(
        name_caches: ThreadedNSNameCachesConfig,
        client: UnicastClientConfig<
            ProcessorConfig<Session>,
            Seal,
            Payload,
            Effect,
            Res,
            Err,
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >
    ) -> Self {
        ProcessorClientConfig {
            client: client,
            name_caches: name_caches
        }
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        ThreadedNSNameCachesConfig,
        UnicastClientConfig<
            ProcessorConfig<Session>,
            Seal,
            Payload,
            Effect,
            Res,
            Err,
            Hash,
            AuthN,
            ProtoAuthN,
            Unix,
            UDP,
            IDs,
            Resolver,
            Epochs,
            MsgAuthN
        >
    ) {
        (self.name_caches, self.client)
    }
}

impl<Proc> ProcessorConfig<Proc> {
    #[inline]
    pub fn new(
        proc: Proc,
        resubmit: Retry
    ) -> Self {
        ProcessorConfig {
            proc: proc,
            resubmit: resubmit
        }
    }

    #[inline]
    pub fn take(self) -> (Proc, Retry) {
        (self.proc, self.resubmit)
    }
}
