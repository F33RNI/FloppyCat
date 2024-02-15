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
import queue
import shutil
import time
from typing import Dict

import LoggingHandler

from _control_values import PROCESS_CONTROL_WORK, PROCESS_CONTROL_PAUSE, PROCESS_CONTROL_EXIT

# Timeout waiting for data from filepath_queue
QUEUE_TIMEOUT = 5


def make_dirs(
    input_dirs_tree: Dict,
    output_dir: str,
    target_dir_path_rel: str,
    stats_created_dirs_ok_value: multiprocessing.Value,
    stats_created_dirs_error_value: multiprocessing.Value,
) -> bool:
    """Creates tree of directories copying their permissions mask from input_dirs_tree

    Args:
        input_dirs_tree (Dict): dictionary of "dirs" dictionary of input tree (to read permissions masks)
        output_dir (str): path to the output (backup) directory
        target_dir_path_rel (str): relative to output_dir path to target directory to create
        stats_created_dirs_ok_value (multiprocessing.Value): counter of total successful mkdirs calls
        stats_created_dirs_error_value (multiprocessing.Value): counter of total unsuccessful mkdirs calls

    Returns:
        bool: True if target directory created successfully, False if not
    """
    # Convert target directory (aka final) path into absolute one
    target_dir_path_abs = os.path.join(output_dir, target_dir_path_rel)

    # We don't need to do anything if it's a directory and it exists
    if os.path.exists(target_dir_path_abs) and os.path.isdir(target_dir_path_abs):
        return True

    # Remove mask from current process
    original_umask = -1
    try:
        original_umask = os.umask(0)
    except:
        pass

    # Split our target directory path into parts
    target_dir_path_rel_parts = target_dir_path_rel.split(os.path.sep)

    # List that will gradually filled with target path parts
    dir_to_make_rel_parts = []

    # Iterate each part
    for target_dir_path_rel_part in target_dir_path_rel_parts:
        # Append to list and create a path to a directory that we need to create
        dir_to_make_rel_parts.append(target_dir_path_rel_part)
        dir_to_make_rel = os.path.normpath(os.path.sep.join(dir_to_make_rel_parts))

        # Convert it into absolute path
        dir_to_make_abs = os.path.join(output_dir, dir_to_make_rel)

        # Check and skip if directory we're trying to make already exists
        if os.path.exists(dir_to_make_abs) and os.path.isdir(dir_to_make_abs):
            continue

        # Try to find it in parsed input tree of directories
        if dir_to_make_rel in input_dirs_tree:
            # Extract permissions mode
            st_mode = input_dirs_tree[dir_to_make_rel]["mode"]

        # Seems strange! Log as warning
        else:
            st_mode = -1
            logging.warning(f"Couldn't find {dir_to_make_rel} directory inside parsed input tree!")

        # Try to create directories with specified mode
        try:
            # Use os.makedirs instead of os.mkdir just in case
            if st_mode != -1:
                # Create directory with desired mask
                os.makedirs(dir_to_make_abs, mode=st_mode)

            # Leave it as is (no mode specified)
            else:
                os.makedirs(dir_to_make_abs)

            # Increment stats counter
            with stats_created_dirs_ok_value.get_lock():
                stats_created_dirs_ok_value.value += 1

        # Log error and increment error counter
        except Exception as e:
            logging.error(
                f"Error making {dir_to_make_abs} directory with mode {st_mode if st_mode != -1 else '-'}: {str(e)}"
            )
            with stats_created_dirs_error_value.get_lock():
                stats_created_dirs_error_value.value += 1

    # Restore mask of current process
    if original_umask != -1:
        try:
            os.umask(original_umask)
        except:
            pass

    # Finally, check again
    if os.path.exists(target_dir_path_abs) and os.path.isdir(target_dir_path_abs):
        return True
    return False


def copy_entries(
    filepaths_queue: multiprocessing.Queue,
    checksums_input: Dict,
    checksums_output: Dict,
    input_dirs_tree: Dict,
    output_dir: str,
    follow_symlinks: bool,
    stats_copied_ok_value: multiprocessing.Value,
    stats_copied_error_value: multiprocessing.Value,
    stats_created_dirs_ok_value: multiprocessing.Value,
    stats_created_dirs_error_value: multiprocessing.Value,
    stats_created_symlinks_value: multiprocessing.Value,
    control_value: multiprocessing.Value or None = None,
    logging_queue: multiprocessing.Queue or None = None,
) -> None:
    """Process body to copy input files and directories to the backup output_dir

    Args:
        filepaths_queue (multiprocessing.Queue): queue of non-skipped files and symlink to to copy
        (path relative to root, root dir)
        checksums_input (Dict): checksums of input files
        checksums_output (Dict): checksums of output files
        {
            "file_1_relative_to_root_path": {
                "root": "file_1_root_directory",
                "checksum": "file_1_checksum"
            },
            "file_2_relative_to_root_path": {
                "root": "file_2_root_directory",
                "checksum": "file_2_checksum"
            }
        }
        input_dirs_tree (Dict): dictionary of "dirs" dictionary of input tree (to read permissions masks)
        output_dir (str): path to the output (backup) directory
        follow_symlinks (bool): False to copy symlinks themselves instead of referenced files
        stats_copied_ok_value (multiprocessing.Value): counter of total successful copy calls
        stats_copied_error_value (multiprocessing.Value): counter of total unsuccessful copy calls
        stats_created_dirs_ok_value (multiprocessing.Value): counter of total successful mkdirs calls
        stats_created_dirs_error_value (multiprocessing.Value): counter of total unsuccessful mkdirs calls
        stats_created_symlinks_value (multiprocessing.Value): counter of total created symlinks
        control_value (multiprocessing.Value or None, optional): value (int) to pause / cancel process
        logging_queue (multiprocessing.Queue or None, optional): logging queue to accept logs
    """
    # Setup logging
    if logging_queue is not None:
        LoggingHandler.worker_configurer(logging_queue, False)

    # Log current PID
    current_pid = multiprocessing.current_process().pid
    if logging_queue is not None:
        logging.info(f"copy_entries() with PID {current_pid} started")

    while True:
        # Check control
        if control_value is not None:
            # Get current control value
            with control_value.get_lock():
                control_value_ = control_value.value

            # Pause
            if control_value_ == PROCESS_CONTROL_PAUSE:
                if logging_queue is not None:
                    logging.info(f"copy_entries() with PID {current_pid} paused")

                # Sleep loop
                while True:
                    # Retrieve updated value
                    with control_value.get_lock():
                        control_value_ = control_value.value

                    # Resume?
                    if control_value_ == PROCESS_CONTROL_WORK:
                        if logging_queue is not None:
                            logging.info(f"copy_entries() with PID {current_pid} resumed")
                        break

                    # Exit?
                    elif control_value_ == PROCESS_CONTROL_EXIT:
                        if logging_queue is not None:
                            logging.info(f"copy_entries() with PID {current_pid} exited upon request")
                        return

                    # Sleep some time
                    time.sleep(0.1)

            # Exit
            elif control_value_ == PROCESS_CONTROL_EXIT:
                if logging_queue is not None:
                    logging.info(f"copy_entries() with PID {current_pid} exited upon request")
                return

        # Retrieve from queue and exit process on timeout
        try:
            filepath_rel, filepath_root_dir = filepaths_queue.get(block=True, timeout=QUEUE_TIMEOUT)
        except queue.Empty:
            if logging_queue is not None:
                logging.info(f"No more files to copy! copy_entries() with PID {current_pid} exited")
            return

        try:
            # Convert to absolute path
            input_file_abs = os.path.join(filepath_root_dir, filepath_rel)

            # Skip if input file not exists
            if not os.path.exists(input_file_abs):
                continue

            # Find input checksum
            checksum_input = None
            if filepath_rel in checksums_input:
                checksum_input = checksums_input[filepath_rel]["checksum"]

            # Find output checksum
            checksum_output = None
            if filepath_rel in checksums_output:
                checksum_output = checksums_output[filepath_rel]["checksum"]

            # Generate output absolute path
            output_path_abs = os.path.join(output_dir, filepath_rel)

            # Skip if file exists and checksums are equal
            if (
                os.path.exists(output_path_abs)
                and checksum_input
                and checksum_output
                and checksum_output == checksum_input
            ):
                continue

            # Try to create directories if not exist and skip if failed
            output_file_base_dir = os.path.dirname(output_path_abs)
            if not os.path.exists(output_file_base_dir):
                if not make_dirs(
                    input_dirs_tree,
                    output_dir,
                    os.path.relpath(output_file_base_dir, output_dir),
                    stats_created_dirs_ok_value,
                    stats_created_dirs_error_value,
                ):
                    continue

            # Copy symlink (create a new one)
            if not follow_symlinks and os.path.islink(input_file_abs):
                link_to = os.readlink(input_file_abs)

                # Ignore if already exists
                if (
                    os.path.exists(output_path_abs)
                    and os.path.islink(output_path_abs)
                    and os.readlink(output_path_abs) == link_to
                ):
                    continue

                # Create symlink
                os.symlink(link_to, output_path_abs, target_is_directory=os.path.isdir(link_to))
                with stats_created_symlinks_value.get_lock():
                    stats_created_symlinks_value.value += 1

            # Copy file
            else:
                shutil.copy(input_file_abs, output_path_abs, follow_symlinks=follow_symlinks)
                with stats_copied_ok_value.get_lock():
                    stats_copied_ok_value.value += 1

        # Error occurred -> log error and increment error counter
        except Exception as e:
            if logging_queue is not None:
                logging.error(f"Error copying {input_file_abs}: {str(e)}")
            with stats_copied_error_value.get_lock():
                stats_copied_error_value.value += 1
