"""
This file is part of the FloppyCat Simple Backup Utility distribution
(https://github.com/F33RNI/FloppyCat)

Copyright (C) 2023-2024 Fern Lane

This program is free software: you can redistribute it and/or modify it under the terms 
of the GNU Affero General Public License as published by the Free Software Foundation,
either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License long with this program.
If not, see <http://www.gnu.org/licenses/>.
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
