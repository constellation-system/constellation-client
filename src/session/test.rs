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

use crate::component::test::TestBusTypes;
use crate::component::test::TestClientComponentTypes;
use crate::component::test::TestHashBusTypes;
use crate::component::test::TestHashClientComponentTypes;
use crate::session::oneshot::OneShotSessionMsgs;
use crate::session::oneshot::OneShotSessionRecv;
use crate::session::processor::ProcessorSessionMsgs;
use crate::session::processor::ProcessorSessionRecv;
#[cfg(feature = "standalone")]
use crate::standalone::StandaloneCtx;

type TestOneShotBusTypes<Ctx> = TestBusTypes<
    OneShotSessionMsgs<TestClientComponentTypes>,
    OneShotSessionRecv<TestClientComponentTypes>,
    Ctx
>;

type TestProcessorBusTypes<Proc, Ctx> = TestHashBusTypes<
    ProcessorSessionMsgs<TestHashClientComponentTypes, Proc>,
    ProcessorSessionRecv<TestHashClientComponentTypes, Proc>,
    Ctx
>;

#[cfg(feature = "standalone")]
pub type StandaloneTestOneShotBusTypes = TestOneShotBusTypes<StandaloneCtx>;

#[cfg(feature = "standalone")]
pub type StandaloneTestProcessorBusTypes<Proc> =
    TestProcessorBusTypes<Proc, StandaloneCtx>;
