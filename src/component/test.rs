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

use std::convert::Infallible;
use std::marker::PhantomData;
use std::net::SocketAddr;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::BasicAuthNed;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::authn::basic::BasicAuthN;
use constellation_auth::config::BasicAuthNConfig;
use constellation_channels::config::CompoundFarChannelXfrmPeerAddr;
use constellation_channels::config::CompoundFarEndpoint;
use constellation_channels::config::ResolverConfig;
use constellation_channels::far::compound::CompoundFlow;
use constellation_channels::far::types::CompoundFarChannelsBasicAuthNedLargeObjChan;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::MixedResolver;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::net::PassthruDatagramXfrm;
use constellation_common::net::PassthruDatagramXfrmParam;
use constellation_common::unix::UnixSocketPath;
use constellation_component_common::bus::types::MulticastLargeObjBusTypes;
use constellation_component_common::bus::types::UnicastLargeObjBusTypes;
use constellation_component_common::config::XactBatchCodecConfig;
use constellation_component_common::xact::XactHashBatch;
use constellation_component_common::xact::test::TestBatch;
use constellation_component_common::xact::test::TestBatchCodec;
use constellation_component_common::xact::test::TestBatchCodecCreateError;
use constellation_component_common::xact::test::TestBatchCodecEncodeError;
use constellation_component_common::xact::test::TestHashBatch;
use constellation_component_common::xact::test::TestHashBatchCodec;
use constellation_component_common::xact::test::TestEffects;
use constellation_component_common::xact::test::TestEffectsCodec;
use constellation_component_common::xact::test::TestError;
use constellation_component_common::xact::test::TestErrorCodec;
use constellation_component_common::xact::test::TestPayload;
use constellation_component_common::xact::test::TestPayloadCodec;
use constellation_component_common::xact::test::TestResult;
use constellation_component_common::xact::test::TestResultCodec;
use constellation_component_common::xact::test::TestSeal;
use constellation_component_common::xact::test::TestSealCodec;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::threads::poll::MsgsSetNotify;

use crate::component::ClientComponentTypes;
use crate::component::HashClientComponentTypes;

pub struct TestClientComponentTypes;
pub struct TestHashClientComponentTypes;

pub struct TestBusTypes<Msgs, Recv, Ctx>
where
    Msgs: LargeObjMsgs<SHA3Algo, TestBatch> + MsgsSetNotify + Send,
    Recv: Clone + AuthNMsgRecv<String, BasicAuthNed<String, TestBatch>> + Send,
    Ctx: NSNameCachesCtx + Send {
    msgs: PhantomData<Msgs>,
    recv: PhantomData<Recv>,
    ctx: PhantomData<Ctx>
}

pub struct TestHashBusTypes<Msgs, Recv, Ctx>
where
    Msgs: LargeObjMsgs<SHA3Algo, TestHashBatch> + MsgsSetNotify + Send,
    Recv: Clone
        + AuthNMsgRecv<String, BasicAuthNed<String, TestHashBatch>> + Send,
    Ctx: NSNameCachesCtx + Send {
    msgs: PhantomData<Msgs>,
    recv: PhantomData<Recv>,
    ctx: PhantomData<Ctx>
}

impl ClientComponentTypes for TestClientComponentTypes {
    type RoundID = u128;
    type MsgPrin = String;
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type Seal = TestSeal;
    type Payload = TestPayload;
    type Effect = TestEffects;
    type Result = TestResult;
    type Error = TestError;
    type SealCodec = TestSealCodec;
    type SealCodecConfig = ();
    type SealCodecCreateError = Infallible;
    type PayloadCodec = TestPayloadCodec;
    type PayloadCodecConfig = ();
    type PayloadCodecCreateError = Infallible;
    type EffectCodec = TestEffectsCodec;
    type EffectCodecConfig = ();
    type EffectCodecCreateError = Infallible;
    type ResultCodec = TestResultCodec;
    type ResultCodecConfig = ();
    type ResultCodecCreateError = Infallible;
    type ErrorCodec = TestErrorCodec;
    type ErrorCodecConfig = ();
    type ErrorCodecCreateError = Infallible;
    type AuthNMsg = BasicAuthNed<
        String,
        TestBatch
    >;
}

impl HashClientComponentTypes for TestHashClientComponentTypes {
    type RoundID = u128;
    type MsgPrin = String;
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type Seal = TestSeal;
    type Payload = TestPayload;
    type Effect = TestEffects;
    type Result = TestResult;
    type Error = TestError;
    type SealCodec = TestSealCodec;
    type SealCodecConfig = ();
    type SealCodecCreateError = Infallible;
    type PayloadCodec = TestPayloadCodec;
    type PayloadCodecConfig = ();
    type PayloadCodecCreateError = Infallible;
    type EffectCodec = TestEffectsCodec;
    type EffectCodecConfig = ();
    type EffectCodecCreateError = Infallible;
    type ResultCodec = TestResultCodec;
    type ResultCodecConfig = ();
    type ResultCodecCreateError = Infallible;
    type ErrorCodec = TestErrorCodec;
    type ErrorCodecConfig = ();
    type ErrorCodecCreateError = Infallible;
    type AuthNMsg = BasicAuthNed<
        String,
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

impl<Msgs, Recv, Ctx> UnicastLargeObjBusTypes<Ctx>
    for TestBusTypes<Msgs, Recv, Ctx>
where
    Msgs: LargeObjMsgs<SHA3Algo, TestBatch> + MsgsSetNotify + Send,
    Recv: Clone + AuthNMsgRecv<String, BasicAuthNed<String, TestBatch>> + Send,
    Ctx: 'static + NSNameCachesCtx + Send {
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, TestBatch>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = TestBatchCodec;
    type DecoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type DecoderCreateError = TestBatchCodecCreateError;
    type Encoder = TestBatchCodec;
    type EncoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type EncoderCreateError = TestBatchCodecCreateError;
    type EncodeError = TestBatchCodecEncodeError;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type InMsg = TestBatch;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth = PassthruMsgAuthN<TestBatch, String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = Msgs;
    type Recv = Recv;
    type Origin = CompoundFarEndpoint;
    type OutMsg = TestBatch;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = TestBatch;
}

impl<Msgs, Recv, Ctx> UnicastLargeObjBusTypes<Ctx>
    for TestHashBusTypes<Msgs, Recv, Ctx>
where
    Msgs: LargeObjMsgs<SHA3Algo, TestHashBatch> + MsgsSetNotify + Send,
    Recv: Clone
    + AuthNMsgRecv<String, BasicAuthNed<String, TestHashBatch>>
    + Send,
    Ctx: 'static + NSNameCachesCtx + Send {
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, TestHashBatch>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = TestHashBatchCodec;
    type DecoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type DecoderCreateError = TestBatchCodecCreateError;
    type Encoder = TestHashBatchCodec;
    type EncoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type EncoderCreateError = TestBatchCodecCreateError;
    type EncodeError = TestBatchCodecEncodeError;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type InMsg = TestHashBatch;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth = PassthruMsgAuthN<TestHashBatch, String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = Msgs;
    type Recv = Recv;
    type Origin = CompoundFarEndpoint;
    type OutMsg = TestHashBatch;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = TestHashBatch;
}

impl<Msgs, Recv, Ctx> MulticastLargeObjBusTypes<Ctx>
    for TestBusTypes<Msgs, Recv, Ctx>
where
    Msgs: LargeObjMsgs<SHA3Algo, TestBatch> + MsgsSetNotify + Send,
    Recv: Clone + AuthNMsgRecv<String, BasicAuthNed<String, TestBatch>> + Send,
    Ctx: 'static + NSNameCachesCtx + Send {
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, TestBatch>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = TestBatchCodec;
    type DecoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type DecoderCreateError = TestBatchCodecCreateError;
    type Encoder = TestBatchCodec;
    type EncoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type EncoderCreateError = TestBatchCodecCreateError;
    type EncodeError = TestBatchCodecEncodeError;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type InMsg = TestBatch;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth = PassthruMsgAuthN<TestBatch, String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = Msgs;
    type Recv = Recv;
    type Origin = CompoundFarEndpoint;
    type OutMsg = TestBatch;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = TestBatch;
}

impl<Msgs, Recv, Ctx> MulticastLargeObjBusTypes<Ctx>
    for TestHashBusTypes<Msgs, Recv, Ctx>
where
    Msgs: LargeObjMsgs<SHA3Algo, TestHashBatch> + MsgsSetNotify + Send,
    Recv: Clone
    + AuthNMsgRecv<String, BasicAuthNed<String, TestHashBatch>>
    + Send,
    Ctx: 'static + NSNameCachesCtx + Send {
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, TestHashBatch>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = TestHashBatchCodec;
    type DecoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type DecoderCreateError = TestBatchCodecCreateError;
    type Encoder = TestHashBatchCodec;
    type EncoderConfig = XactBatchCodecConfig<(), (), (), (), ()>;
    type EncoderCreateError = TestBatchCodecCreateError;
    type EncodeError = TestBatchCodecEncodeError;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type InMsg = TestHashBatch;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth = PassthruMsgAuthN<TestHashBatch, String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = Msgs;
    type Recv = Recv;
    type Origin = CompoundFarEndpoint;
    type OutMsg = TestHashBatch;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = TestHashBatch;
}
