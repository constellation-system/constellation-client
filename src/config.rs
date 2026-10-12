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

use constellation_component_common::config::MulticastLargeObjBusConfig;
use constellation_component_common::config::UnicastLargeObjBusConfig;
use constellation_component_common::config::XactBatchCodecConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "multicast-client")]
#[serde(rename_all = "kebab-case")]
pub struct MulticastClientConfig<
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
        String,
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
            String,
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
        String,
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
            String,
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
