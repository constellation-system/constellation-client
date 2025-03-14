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

use constellation_channels::config::ChannelRegistryChannelsConfig;
use constellation_channels::config::ChannelRegistryConfig;
use constellation_channels::config::CompoundFarChannelConfig;
use constellation_channels::config::CompoundFarEndpoint;
use constellation_channels::config::CompoundXfrmCreateParam;
use constellation_channels::config::ThreadedFlowsParams;
use constellation_channels::config::ThreadedNSNameCachesConfig;
use constellation_client::config::MulticastClientConfig;
use constellation_common::codec::DatagramCodec;
use constellation_common::ids::AscendingCount;
use constellation_common::ids::IDGen;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjMsgCodec;
use serde::Deserialize;
use serde::Serialize;

pub type RegistryConfig = ChannelRegistryConfig<
    CompoundFarChannelConfig,
    ThreadedFlowsParams,
    CompoundXfrmCreateParam<(), ()>
>;

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "example")]
#[serde(rename_all = "kebab-case")]
pub struct CmdlineConfig {
    /// Name cache configuration.
    #[serde(default)]
    name_caches: ThreadedNSNameCachesConfig,
    /// Channel registry configuration.
    #[serde(flatten)]
    registry: RegistryConfig,
    #[serde(flatten)]
    multicast: MulticastClientConfig<
        String,
        ChannelRegistryChannelsConfig<
            <LargeObjMsgCodec as DatagramCodec<LargeObjMsg>>::Param
        >,
        <AscendingCount as IDGen>::Config,
        CompoundFarEndpoint
    >,
    #[serde(default)]
    session: ()
}

impl CmdlineConfig {
    pub fn take(
        self
    ) -> (ThreadedNSNameCachesConfig,
          RegistryConfig,
          MulticastClientConfig<
              String,
              ChannelRegistryChannelsConfig<
                      <LargeObjMsgCodec as DatagramCodec<LargeObjMsg>>::Param
                      >,
              <AscendingCount as IDGen>::Config,
              CompoundFarEndpoint
          >,
          ())
    {
        (self.name_caches, self.registry, self.multicast, self.session)
    }
}
