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
use std::hash::Hash;

use constellation_auth::authn::AuthNed;
use constellation_auth::authn::AuthNedDestruct;
use constellation_common::codec::Decoder;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_component_common::xact::XactHashBatch;
use constellation_component_common::xact::XactBatch;
use serde::Deserialize;
use serde::Serialize;

pub mod multicast;
pub mod test;
pub mod unicast;

pub trait ClientComponentTypes {
    type RoundID: Clone + Display + From<u128> + Into<u128> + Send;
    type MsgPrin: Clone + Debug + Display + Eq + Hash;
    type Hash: Clone + Default
        + HashAlgo<HashID = Self::HashID>
        + Create<Config = Self::HashConfig,
                 CreateError = Self::HashCreateError>
        + Send;
    type HashConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type HashCreateError: Debug + Display;
    type HashID: Clone + Debug + Display + Eq + Hash + HashID + Send;
    type Seal: Clone + Display + Send;
    type Payload: Clone + Display + Send;
    type Effect: Clone + Display + Send;
    type Result: Clone + Display + Send;
    type Error: Clone + Display + Send;
    type SealCodec: Create<Config = Self::SealCodecConfig,
                           CreateError = Self::SealCodecCreateError>
        + Encoder<Self::Seal> + Decoder<Self::Seal>;
    type SealCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type SealCodecCreateError: Debug + Display;
    type PayloadCodec: Create<Config = Self::PayloadCodecConfig,
                              CreateError = Self::PayloadCodecCreateError>
        + Encoder<Self::Payload> + Decoder<Self::Payload>;
    type PayloadCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type PayloadCodecCreateError: Debug + Display;
    type EffectCodec: Create<Config = Self::EffectCodecConfig,
                             CreateError = Self::EffectCodecCreateError>
        + Encoder<Self::Effect> + Decoder<Self::Effect>;
    type EffectCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type EffectCodecCreateError: Debug + Display;
    type ResultCodec: Create<Config = Self::ResultCodecConfig,
                             CreateError = Self::ResultCodecCreateError>
        + Encoder<Self::Result> + Decoder<Self::Result>;
    type ResultCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type ResultCodecCreateError: Debug + Display;
    type ErrorCodec: Create<Config = Self::ErrorCodecConfig,
                            CreateError = Self::ErrorCodecCreateError>
        + Encoder<Self::Error> + Decoder<Self::Error>;
    type ErrorCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type ErrorCodecCreateError: Debug + Display;
    type AuthNMsg: AuthNed<Self::MsgPrin>
        + AuthNedDestruct<
            Self::MsgPrin,
            XactBatch<
                Self::RoundID,
                Self::HashID,
                Self::Seal,
                Self::Payload,
                Self::Effect,
                Self::Result,
                Self::Error
            >
        >;
}

pub trait HashClientComponentTypes {
    type RoundID: Clone + Display + From<u128> + Into<u128> + Send;
    type MsgPrin: Clone + Debug + Display + Eq + Hash;
    type Hash: Clone + Default
        + HashAlgo<HashID = Self::HashID>
        + Create<Config = Self::HashConfig,
                 CreateError = Self::HashCreateError>
        + Send;
    type HashConfig: Default + for<'a> Deserialize<'a> + Serialize;
    type HashCreateError: Debug + Display;
    type HashID: Clone + Debug + Display + Eq + Hash + HashID + Send;
    type Seal: Clone + Display + Send;
    type Payload: Clone + Display + Send;
    type Effect: Clone + Display + Send;
    type Result: Clone + Display + Send;
    type Error: Clone + Display + Send;
    type SealCodec: Create<Config = Self::SealCodecConfig,
                           CreateError = Self::SealCodecCreateError>
        + Encoder<Self::Seal> + Decoder<Self::Seal>;
    type SealCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type SealCodecCreateError: Debug + Display;
    type PayloadCodec: Create<Config = Self::PayloadCodecConfig,
                              CreateError = Self::PayloadCodecCreateError>
        + Encoder<Self::Payload> + Decoder<Self::Payload>;
    type PayloadCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type PayloadCodecCreateError: Debug + Display;
    type EffectCodec: Create<Config = Self::EffectCodecConfig,
                             CreateError = Self::EffectCodecCreateError>
        + Encoder<Self::Effect> + Decoder<Self::Effect>;
    type EffectCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type EffectCodecCreateError: Debug + Display;
    type ResultCodec: Create<Config = Self::ResultCodecConfig,
                             CreateError = Self::ResultCodecCreateError>
        + Encoder<Self::Result> + Decoder<Self::Result>;
    type ResultCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type ResultCodecCreateError: Debug + Display;
    type ErrorCodec: Create<Config = Self::ErrorCodecConfig,
                            CreateError = Self::ErrorCodecCreateError>
        + Encoder<Self::Error> + Decoder<Self::Error>;
    type ErrorCodecConfig: Clone + Default + for<'a> Deserialize<'a> + Serialize;
    type ErrorCodecCreateError: Debug + Display;
    type AuthNMsg: AuthNed<Self::MsgPrin>
        + AuthNedDestruct<
            Self::MsgPrin,
            XactHashBatch<
                Self::RoundID,
                Self::HashID,
                Self::Seal,
                Self::Payload,
                Self::Effect,
                Self::Result,
                Self::Error
            >
        >;
}
