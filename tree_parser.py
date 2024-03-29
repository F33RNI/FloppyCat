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

import logging
import multiprocessing
import os
import pathlib
import queue
import time
from typing import List

import LoggingHandler

from _control_values import PROCESS_CONTROL_WORK, PROCESS_CONTROL_PAUSE, PROCESS_CONTROL_EXIT

# Timeout waiting for data from directories_to_parse_queue
QUEUE_TIMEOUT = 2

# Definitions for parsed_queue
PATH_FILE = 0
PATH_DIR = 1
PATH_SYMLINK = 2
PATH_UNKNOWN = 3


def tree_parser(
    directories_to_parse_queue: multiprocessing.Queue,
    parsed_queue: multiprocessing.Queue,
    skipped_entries_abs: List[str],
    follow_symlinks: bool,
    stats_tree_parsed_dirs: multiprocessing.Value,
    stats_tree_parsed_files: multiprocessing.Value,
    stats_tree_parsed_symlinks: multiprocessing.Value,
    stats_tree_parsed_unknown: multiprocessing.Value,
    control_value: multiprocessing.Value or None = None,
    logging_queue: multiprocessing.Queue or None = None,
) -> None:
    """Process body to parse directory tree

    NOTE: first root directory will not be added to the parsed_queue!

    Args:
        directories_to_parse_queue (multiprocessing.Queue): queue of directories to parse.
        Put your initial dir here. Format: (relative parent dir or "", absolute path of root directory)
        parsed_queue (multiprocessing.Queue): parsing results as tuples
        (relative path, root dir path, PATH_..., is empty directory)
        skipped_entries_abs (List[str]): list of normalized absolute paths to skip,
        follow_symlinks (bool): True to follow symlinks, false to ignore them
        stats_tree_parsed_dirs (multiprocessing.Value): counter of total successfully parsed directories
        stats_tree_parsed_files (multiprocessing.Value): counter of total successfully parsed files
        stats_tree_parsed_symlinks (multiprocessing.Value): counter of total successfully parsed symlinks
        stats_tree_parsed_unknown (multiprocessing.Value): counter of total parsed unknown entries
        control_value (multiprocessing.Value or None, optional): value (int) to pause / cancel process
        logging_queue (multiprocessing.Queue or None, optional): logging queue to accept logs
    """
    # Setup logging
    if logging_queue is not None:
        LoggingHandler.worker_configurer(logging_queue, False)

    # Log current PID
    current_pid = multiprocessing.current_process().pid
    if logging_queue is not None:
        logging.info(f"tree_parser() with PID {current_pid} started")

    while True:
        # Check control
        if control_value is not None:
            # Get current control value
            with control_value.get_lock():
                control_value_ = control_value.value

            # Pause
            if control_value_ == PROCESS_CONTROL_PAUSE:
                if logging_queue is not None:
                    logging.info(f"tree_parser() with PID {current_pid} paused")

                # Sleep loop
                while True:
                    # Retrieve updated value
                    with control_value.get_lock():
                        control_value_ = control_value.value

                    # Resume?
                    if control_value_ == PROCESS_CONTROL_WORK:
                        if logging_queue is not None:
                            logging.info(f"tree_parser() with PID {current_pid} resumed")
                        break

                    # Exit?
                    elif control_value_ == PROCESS_CONTROL_EXIT:
                        if logging_queue is not None:
                            logging.info(f"tree_parser() with PID {current_pid} exited upon request")
                        return

                    # Sleep some time
                    time.sleep(0.1)

            # Exit
            elif control_value_ == PROCESS_CONTROL_EXIT:
                if logging_queue is not None:
                    logging.info(f"tree_parser() with PID {current_pid} exited upon request")
                return

        # Retrieve from queue and exit process on timeout
        try:
            parent_dir, root_dir = directories_to_parse_queue.get(block=True, timeout=QUEUE_TIMEOUT)
        except queue.Empty:
            if logging_queue is not None:
                logging.info(f"No more directories to parse! tree_parser() with PID {current_pid} exited")
            return

        # Convert parent dir to absolute path
        parent_dir_abs = os.path.join(root_dir, parent_dir)

        # Ignore if found in skipped paths
        if parent_dir_abs in skipped_entries_abs:
            continue

        # Try to create generator to iterate all files and dirs inside this directory
        try:
            parent_dir_abs_generator = pathlib.Path(parent_dir_abs).iterdir()
        except Exception as e:
            if logging_queue is not None:
                logging.error(f"Error parsing {parent_dir_abs} tree: {str(e)}")
            continue

        # Iterate all files and dirs
        while True:
            # Try to get next path
            try:
                dir_or_file = next(parent_dir_abs_generator)

            # No more paths -> exit
            except StopIteration:
                break

            # Error occurred -> log error
            except Exception as e:
                if logging_queue is not None:
                    logging.error(f"Error iterating next path: {str(e)}")
                continue

            # Parse it
            try:
                # Ignore if found in skipped paths
                if os.path.normpath(str(dir_or_file)) in skipped_entries_abs:
                    continue

                # Find path relative to root
                dir_or_file_rel = os.path.relpath(dir_or_file, root_dir)

                # Increment symlinks counter
                if dir_or_file.is_symlink():
                    with stats_tree_parsed_symlinks.get_lock():
                        stats_tree_parsed_symlinks.value += 1

                # Symlink and not follow it -> just parse as symlink
                if dir_or_file.is_symlink() and not follow_symlinks:
                    # (relative path, root dir path, PATH_..., is empty directory, permission mask)
                    parsed_queue.put((dir_or_file_rel, root_dir, PATH_SYMLINK, False, -1))

                # Just file -> put to parsed queue
                elif dir_or_file.is_file():
                    # (relative path, root dir path, PATH_..., is empty directory, permission mask)
                    parsed_queue.put((dir_or_file_rel, root_dir, PATH_FILE, False, -1))

                    # Increment counter
                    with stats_tree_parsed_files.get_lock():
                        stats_tree_parsed_files.value += 1

                # Directory -> put to parsed queue and request recursive parse
                elif dir_or_file.is_dir():
                    # Check if empty ignoring errors (just in case)
                    try:
                        is_empty = not any(dir_or_file.iterdir())
                    except:
                        is_empty = False

                    # Try to get permission mask
                    try:
                        st_mask = os.stat(dir_or_file).st_mode & 0o777
                    except:
                        st_mask = -1

                    # (relative path, root dir path, PATH_..., is empty directory, permission mask)
                    parsed_queue.put((dir_or_file_rel, root_dir, PATH_DIR, is_empty, st_mask))

                    # Put again in recursion if not empty with the same root
                    if not is_empty:
                        directories_to_parse_queue.put((dir_or_file_rel, root_dir))

                    # Increment counter
                    with stats_tree_parsed_dirs.get_lock():
                        stats_tree_parsed_dirs.value += 1

                # Not a file or directory -> put to parsed queue as PATH_UNKNOWN
                else:
                    # Log it
                    logging.warning(f"'{dir_or_file}' is not a symbolic link, file or directory")

                    # (relative path, root dir path, PATH_..., is empty directory, permission mask)
                    parsed_queue.put((dir_or_file_rel, root_dir, PATH_UNKNOWN, False, -1))

                    # Increment unknown counter
                    with stats_tree_parsed_unknown.get_lock():
                        stats_tree_parsed_unknown.value += 1

            # Error occurred -> log error
            except Exception as e:
                if logging_queue is not None:
                    logging.error(f"Error parsing {str(dir_or_file)} path: {str(e)}")
