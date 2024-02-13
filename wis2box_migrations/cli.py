###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################
import importlib
import logging

import click
from wis2box_migrations import __version__
LOGGER = logging.getLogger(__name__)


def cli_option_verbosity(f):
    options = ["DEBUG", "INFO", "WARNING", "ERROR"]

    def callback(ctx, param, value):
        if value is not None:
            LOGGER.setlevel(getattr(logging, value))
        return True

    return click.option("--verbosity", "-v",
                        type=click.Choice(options),
                        help="Verbosity",
                        callback=callback)(f)


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


@click.command("run")
@click.argument("wis2box_version", nargs=1)
@click.option("--dryrun", "dryrun", help="Dry run, print result to screen",
              required=False, default=False)
def run(wis2box_version, dryrun):
    # get version, replace periods with underscore
    v = wis2box_version.replace(".", "_")
    # load migration runner
    m = importlib.import_module(f"wis2box_migrations.migrations.{v}")
    migrate = getattr(m, "migrate")
    # now run migration
    migrate(dryrun)


cli.add_command(run)
