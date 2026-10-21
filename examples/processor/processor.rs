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

use constellation_client::component::test::TestHashClientComponentTypes;
use constellation_client::session::processor::Processor;
use constellation_client::session::processor::ProcessorRunError;
use constellation_client::session::processor::SimpleStandaloneProcessor;
use constellation_client::session::test::StandaloneTestProcessorBusTypes;
use constellation_common::config::Create;
use constellation_component_common::xact::XactCommittedEffects;
use constellation_component_common::xact::XactLinPoint;
use constellation_component_common::xact::test::TestEffects;
use constellation_component_common::xact::test::TestError;
use constellation_component_common::xact::test::TestPayload;
use constellation_component_common::xact::test::TestResult;
use log::debug;

#[derive(Clone)]
pub struct ExampleProcessor;

pub type StandaloneExampleProcessor = SimpleStandaloneProcessor<
    TestHashClientComponentTypes,
    StandaloneTestProcessorBusTypes<ExampleProcessor>,
    ExampleProcessor
>;

impl Create for ExampleProcessor {
    type Config = ();
    type CreateError = Infallible;

    #[inline]
    fn create(_config: ()) -> Result<Self, Self::CreateError> {
        Ok(ExampleProcessor)
    }
}

impl Processor<u128, TestPayload, TestEffects, TestResult, TestError>
    for ExampleProcessor
{
    const CONFIG_FILES: &[&str] = &[&"example-processor.conf"];
    const DEFAULT_PIDFILE: &str = "/tmp/example-processor.pid";
    const SERVICE_ID: &str = "org.constellation.example";
    const SERVICE_NAME: &str = "example-processor";

    fn run_uncommitted(
        &mut self,
        req: TestPayload,
        _when: Option<XactLinPoint<u128>>
    ) -> Result<TestResult, ProcessorRunError<TestError>> {
        let (effects, res) = req.take();

        if effects.is_empty() {
            debug!(target: "example-processor",
                   "generating normal result");

            res.map_err(ProcessorRunError::Run)
        } else {
            debug!(target: "example-processor",
                   "transaction causes effects");

            Err(ProcessorRunError::EffectViolation)
        }
    }

    fn run_committed(
        &mut self,
        req: TestPayload,
        effects: Option<XactCommittedEffects<TestEffects>>
    ) -> Result<TestResult, ProcessorRunError<TestError>> {
        let (actual_effects, res) = req.take();

        match effects {
            Some(effects) if effects.hard() => {
                debug!(target: "example-processor",
                       "transaction has effects");

                if actual_effects.is_subset_of(effects.effects()) {
                    res.map_err(ProcessorRunError::Run)
                } else {
                    Err(ProcessorRunError::EffectViolation)
                }
            }
            _ => {
                debug!(target: "example-processor",
                       "generating result");

                res.map_err(ProcessorRunError::Run)
            }
        }
    }
}
