"""
 Copyright (C) 2023 Fern Lane, FloppyCat Simple Backup Utility

 Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

       https://www.gnu.org/licenses/agpl-3.0.en.html

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,

 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR
 OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 OTHER DEALINGS IN THE SOFTWARE.
"""
import argparse
import logging
import multiprocessing
import os
import sys

from _version import __version__
import ConfigManager
import LoggingHandler
import GUIHandler
import Backupper


def get_resource_path(filename_: str) -> str:
    """Converts local file path to absolute path
    (For proper resources loading using pyinstaller)

    Args:
        filename_ (str): local file path

    Returns:
        str: absolute file path
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), filename_))


# Default config file location
CONFIG_FILE = get_resource_path("config.json")


def parse_args() -> argparse.Namespace:
    """Parses cli arguments

    Returns:
          argparse.Namespace: parsed arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        help="config.json file location",
        default=os.getenv("SSB_CONFIG_FILE", CONFIG_FILE),
    )
    parser.add_argument(
        "--enable_console_logging",
        action="store_true",
        help="Specify to enable logging into sys.stdout",
        default=False,
    )
    parser.add_argument("--version", action="version", version=__version__)
    return parser.parse_args()


def main() -> None:
    """Main entry point"""
    # Multiprocessing fix for Windows
    if sys.platform.startswith("win"):
        multiprocessing.freeze_support()

    # Parse arguments
    args = parse_args()

    # Initialize logging, start logging for main process and start listeners
    logging_handler = LoggingHandler.LoggingHandler()
    logging_handler.configure_and_start_listener(args.enable_console_logging)
    LoggingHandler.worker_configurer(logging_handler.queue)

    # Log software version and GitHub link
    logging.info(f"FloppyCat Simple Backup Utility version: {__version__}")
    logging.info("https://github.com/F33RNI/FloppyCat")

    # Load configs
    config_manager = ConfigManager.ConfigManager(args.config)

    # Initialize backupper
    backupper = Backupper.Backupper(config_manager, logging_handler.queue)

    # Initialize GUI
    gui = GUIHandler.GUIHandler(config_manager)

    # Load GUI (blocking) and catch exit code
    exit_code = gui.start_gui(logging_handler, backupper)

    # If we're here, exit requested
    logging.info(f"FloppyCat Simple Backup Utility exited with code: {exit_code}")

    # Finally, stop logging loop
    logging_handler.queue.put(None)

    # Return exit code
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
