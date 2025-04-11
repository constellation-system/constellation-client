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

use constellation_component_common::config::MulticastLargeObjBusConfig;
use constellation_component_common::config::UnicastLargeObjBusConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "multicast-client")]
#[serde(rename_all = "kebab-case")]
pub struct MulticastClientConfig<PartyID, Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default {
    /// Party identitfying this node.
    #[serde(rename = "self")]
    self_party: PartyID,
    #[serde(flatten)]
    multicast: MulticastLargeObjBusConfig<PartyID, Channels, Epochs, Endpoint>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "unicast-client")]
#[serde(rename_all = "kebab-case")]
pub struct UnicastClientConfig<Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default {
    #[serde(flatten)]
    unicast: UnicastLargeObjBusConfig<Channels, Epochs, Endpoint>
}

impl<Channels, Epochs, Endpoint> UnicastClientConfig<Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default
{
    #[inline]
    pub fn new(
        unicast: UnicastLargeObjBusConfig<Channels, Epochs, Endpoint>
    ) -> Self {
        UnicastClientConfig { unicast: unicast }
    }

    #[inline]
    pub fn unicast(
        &self
    ) -> &UnicastLargeObjBusConfig<Channels, Epochs, Endpoint> {
        &self.unicast
    }

    #[inline]
    pub fn take(self) -> UnicastLargeObjBusConfig<Channels, Epochs, Endpoint> {
        self.unicast
    }
}

impl<PartyID, Channels, Epochs, Endpoint>
    MulticastClientConfig<PartyID, Channels, Epochs, Endpoint>
where
    Channels: Default,
    Epochs: Default
{
    #[inline]
    pub fn new(
        self_party: PartyID,
        multicast: MulticastLargeObjBusConfig<
            PartyID,
            Channels,
            Epochs,
            Endpoint
        >
    ) -> Self {
        MulticastClientConfig {
            self_party: self_party,
            multicast: multicast
        }
    }

    #[inline]
    pub fn self_party(&self) -> &PartyID {
        &self.self_party
    }

    #[inline]
    pub fn multicast(
        &self
    ) -> &MulticastLargeObjBusConfig<PartyID, Channels, Epochs, Endpoint> {
        &self.multicast
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        PartyID,
        MulticastLargeObjBusConfig<PartyID, Channels, Epochs, Endpoint>
    ) {
        (self.self_party, self.multicast)
    }
}
