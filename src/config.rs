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

use constellation_component_common::config::MulticastCommConfig;
use constellation_component_common::config::UnicastCommConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "multicast-client")]
#[serde(rename_all = "kebab-case")]
pub struct MulticastClientConfig<Session, PartyID, Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default {
    /// Party identitfying this node.
    #[serde(rename = "self")]
    self_party: PartyID,
    #[serde(flatten)]
    session: Session,
    #[serde(flatten)]
    multicast: MulticastCommConfig<PartyID, Channels, Epochs, Endpoint>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "unicast-client")]
#[serde(rename_all = "kebab-case")]
pub struct UnicastClientConfig<Session, Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default {
    #[serde(flatten)]
    session: Session,
    #[serde(flatten)]
    unicast: UnicastCommConfig<Channels, Epochs, Endpoint>
}

impl<Session, Channels, Epochs, Endpoint>
    UnicastClientConfig<Session, Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default
{
    #[inline]
    pub fn new(
        session: Session,
        unicast: UnicastCommConfig<Channels, Epochs, Endpoint>
    ) -> Self {
        UnicastClientConfig {
            unicast: unicast,
            session: session
        }
    }

    #[inline]
    pub fn session(&self) -> &Session {
        &self.session
    }

    #[inline]
    pub fn unicast(&self) -> &UnicastCommConfig<Channels, Epochs, Endpoint> {
        &self.unicast
    }

    #[inline]
    pub fn take(
        self
    ) -> (Session, UnicastCommConfig<Channels, Epochs, Endpoint>) {
        (self.session, self.unicast)
    }
}

impl<Session, PartyID, Channels, Epochs, Endpoint>
    MulticastClientConfig<Session, PartyID, Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default
{
    #[inline]
    pub fn new(
        self_party: PartyID,
        session: Session,
        multicast: MulticastCommConfig<PartyID, Channels, Epochs, Endpoint>
    ) -> Self {
        MulticastClientConfig {
            self_party: self_party,
            multicast: multicast,
            session: session
        }
    }

    #[inline]
    pub fn self_party(&self) -> &PartyID {
        &self.self_party
    }

    #[inline]
    pub fn session(&self) -> &Session {
        &self.session
    }

    #[inline]
    pub fn multicast(
        &self
    ) -> &MulticastCommConfig<PartyID, Channels, Epochs, Endpoint> {
        &self.multicast
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        PartyID,
        Session,
        MulticastCommConfig<PartyID, Channels, Epochs, Endpoint>
    ) {
        (self.self_party, self.session, self.multicast)
    }
}
