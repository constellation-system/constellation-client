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

use std::convert::Infallible;

use clap::ArgMatches;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_standalone::Standalone;
use constellation_standalone::StandaloneApp;
use log::error;
use log::info;

use crate::config::ExampleConfig;

pub struct StandaloneCmdLine;

impl Standalone for StandaloneCmdLine {
    const NAME: &str = "cmdline";
    const CONFIG_FILES: &[&str] = &[
        "cmdline.conf"
    ];
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );
    type Config = ExampleConfig;
    type CreateCleanup = ();

    fn create(
        _args: ArgMatches,
        _config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        Ok((StandaloneCmdLine, ()))
    }
}

impl StandaloneApp for StandaloneCmdLine {
    type RunErrorCleanup = Infallible;

    fn run(
        self,
        _shutdown: ShutdownFlag
    ) -> Result<(), Self::RunErrorCleanup> {
        info!(target: "example",
              "starting example");

        Ok(())
    }

    fn cleanup(
        _create: Self::CreateCleanup,
    ) {}

    fn cleanup_err(
        _create: Self::CreateCleanup,
        _run: Self::RunErrorCleanup
    ) {
        error!(target: "example",
               "this should never be called");
    }
}
