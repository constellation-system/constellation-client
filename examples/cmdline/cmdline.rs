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
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;

use clap::Arg;
use clap::ArgAction;
use clap::ArgMatches;
use clap::Command;
use constellation_client::component::test::TestClientComponentTypes;
use constellation_client::session::oneshot::OneShot;
use constellation_client::session::oneshot::SimpleStandaloneOneShot;
use constellation_client::session::test::StandaloneTestOneShotBusTypes;
use constellation_common::config::Create;
use constellation_common::version::Version;
use constellation_component_common::xact::XactEffects;
use constellation_component_common::xact::XactUncommittedReq;
use constellation_component_common::xact::test::TestEffects;
use constellation_component_common::xact::test::TestError;
use constellation_component_common::xact::test::TestPayload;
use constellation_component_common::xact::test::TestResult;
use constellation_component_common::xact::test::TestSeal;
use uuid::Uuid;

pub struct ExampleCmdLine;

pub type StandaloneExampleCmdLine = SimpleStandaloneOneShot<
        TestClientComponentTypes,
        StandaloneTestOneShotBusTypes,
        ExampleCmdLine
    >;

const EXAMPLE_SERVICE_NAME: &str = "org.constellation.example";
const EXAMPLE_VERSION: Version = Version::new(0, 0, 0);

#[derive(Clone)]
pub enum CmdlineMode {
    NoEffects {
        hard: bool,
        correct: bool,
        error: bool
    },
    SoftEmptyEffects {
        correct: bool,
        error: bool
    },
    Effects {
        hard: bool,
        correct: bool,
        error: bool
    }
}

#[derive(Debug)]
pub enum ExampleGetArgsError {
    BadCmd {
        cmd: String
    },
    NoCmd
}

impl Create for ExampleCmdLine {
    type Config = ();
    type CreateError = Infallible;

    #[inline]
    fn create(_config: ()) -> Result<Self, Self::CreateError> {
        Ok(ExampleCmdLine)
    }
}

impl OneShot<u128, TestPayload, TestEffects, TestSeal> for ExampleCmdLine {
    const NAME: &str = "example";
    const CONFIG_FILES: &[&str] = &["example-cmdline.yml"];

    type Param = CmdlineMode;
    type GetArgError = ExampleGetArgsError;
    type CreateSealError = Infallible;

    fn get_msg(
        &self,
        param: Self::Param
    ) -> (Uuid, Version, u64, TestPayload, XactEffects<u128, TestEffects>) {
        let uuid =
            Uuid::new_v5(&Uuid::NAMESPACE_DNS, EXAMPLE_SERVICE_NAME.as_bytes());
        let (payload, effects) = param.req();

        (uuid, EXAMPLE_VERSION.clone(), 0, payload, effects)
    }

    fn create_seal(
        &self,
        _req: &XactUncommittedReq<u128, TestPayload, TestEffects>
    ) -> Result<TestSeal, Self::CreateSealError> {
        Ok(TestSeal)
    }

    #[cfg(feature = "standalone")]
    fn get_arg(
        args: ArgMatches
    ) -> Result<Self::Param, Self::GetArgError> {
        match args.subcommand() {
            Some(("none", args)) => {
                let error = args.get_flag("fail");
                let correct = !args.get_flag("violate");

                Ok(CmdlineMode::SoftEmptyEffects {
                    correct: correct,
                    error: error
                })
            }
            Some(("empty", args)) => {
                let hard = args.get_flag("hard");
                let error = args.get_flag("fail");
                let correct = !args.get_flag("violate");

                Ok(CmdlineMode::NoEffects {
                    correct: correct,
                    error: error,
                    hard: hard
                })
            }
            Some(("effects", args)) => {
                let hard = args.get_flag("hard");
                let error = args.get_flag("fail");
                let correct = !args.get_flag("violate");

                Ok(CmdlineMode::Effects {
                    correct: correct,
                    error: error,
                    hard: hard
                })
            }
            Some((cmd, _)) => Err(ExampleGetArgsError::BadCmd {
                cmd: cmd.to_string()
            }),
            None => Err(ExampleGetArgsError::NoCmd)
        }
    }

    fn cmdargs(cmd: Command) -> Command {
        cmd
            .subcommand_required(true)
            .subcommands([
                Command::new("none")
                    .about("Command with no effects set")
                    .args([
                        Arg::new("fail")
                            .long("fail")
                            .help("have the transaction fail")
                            .action(ArgAction::SetTrue),
                        Arg::new("violate")
                            .long("violate")
                            .help("have the transaction violate the \
                                   effect constraints")
                            .action(ArgAction::SetTrue)
                    ]),
                Command::new("empty")
                    .about("Command with an empty effects set")
                    .args([
                        Arg::new("fail")
                            .long("fail")
                            .help("have the transaction fail")
                            .action(ArgAction::SetTrue),
                        Arg::new("violate")
                            .long("violate")
                            .help("have the transaction violate the \
                                   effect constraints")
                            .action(ArgAction::SetTrue),
                        Arg::new("hard")
                            .long("hard")
                            .help("have the transaction generate hard \
                                   effect constraints")
                            .action(ArgAction::SetTrue)
                    ]),
                Command::new("effects")
                    .about("Command with an effects set")
                    .args([
                        Arg::new("fail")
                            .long("fail")
                            .help("have the transaction fail")
                            .action(ArgAction::SetTrue),
                        Arg::new("violate")
                            .long("violate")
                            .help("have the transaction violate the \
                                   effect constraints")
                            .action(ArgAction::SetTrue),
                        Arg::new("hard")
                            .long("hard")
                            .help("have the transaction generate hard \
                                   effect constraints")
                            .action(ArgAction::SetTrue)
                    ]),
            ])
    }
}

impl CmdlineMode {
    fn req(&self,) -> (TestPayload, XactEffects<u128, TestEffects>) {
        match self {
            CmdlineMode::NoEffects {
                hard,
                correct,
                error
            } => {
                let res = if *error {
                    Err(TestError {
                        err: String::from("test error")
                    })
                } else {
                    Ok(TestResult { val: vec![0x10] })
                };
                let payload = if *correct {
                    TestPayload {
                        effects: vec![],
                        res: res
                    }
                } else {
                    TestPayload {
                        effects: vec![0x01],
                        res: res
                    }
                };
                let effects = if *hard {
                    XactEffects::HardNone { when: None }
                } else {
                    XactEffects::SoftNone
                };

                (payload, effects)
            }
            CmdlineMode::Effects {
                hard,
                correct,
                error
            } => {
                let res = if *error {
                    Err(TestError {
                        err: String::from("test error")
                    })
                } else {
                    Ok(TestResult { val: vec![0x10] })
                };
                let payload = if *correct {
                    TestPayload {
                        effects: vec![0x01],
                        res: res
                    }
                } else {
                    TestPayload {
                        effects: vec![0x02],
                        res: res
                    }
                };
                let effects = XactEffects::Effects {
                    effects: TestEffects {
                        effects: vec![0x01]
                    },
                    hard: *hard
                };

                (payload, effects)
            }
            CmdlineMode::SoftEmptyEffects { correct, error } => {
                let res = if *error {
                    Err(TestError {
                        err: String::from("test error")
                    })
                } else {
                    Ok(TestResult { val: vec![0x10] })
                };
                let payload = if *correct {
                    TestPayload {
                        effects: vec![0x01],
                        res: res
                    }
                } else {
                    TestPayload {
                        effects: vec![0x02],
                        res: res
                    }
                };
                let effects = XactEffects::Effects {
                    effects: TestEffects { effects: vec![] },
                    hard: false
                };

                (payload, effects)
            }
        }
    }
}

impl Display for ExampleGetArgsError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ExampleGetArgsError::BadCmd { cmd } =>
                write!(f, "bad effects specifier: {}", cmd),
            ExampleGetArgsError::NoCmd =>
                write!(f, "no effects specified")
        }
    }
}
