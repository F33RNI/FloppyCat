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
from typing import Dict, List

import LoggingHandler

from _control_values import PROCESS_CONTROL_WORK, PROCESS_CONTROL_PAUSE, PROCESS_CONTROL_EXIT

# Timeout waiting for data from output_tree_queue
QUEUE_TIMEOUT = 5


def is_path_relative_to(parent_path: List[str], child_path: List[str]) -> bool:
    """Determines if child_path can be inside (and continue) parent_path

    Args:
        parent_path (List[str]): os.path.normpath("/dir_a/dir_b/dir_c/dir_d").split(os.path.sep)
        child_path (List[str]): os.path.normpath("dir_c/dir_d/dir_e/text.txt").split(os.path.sep)

    Returns:
        bool: True in case of example above
    """
    child_part_index = 0
    parent_part_last_match_index = -1
    child_path_len = len(child_path)
    for i, parent_path_part in enumerate(parent_path):
        if parent_path_part == child_path[child_part_index]:
            child_part_index += 1
            parent_part_last_match_index = i
            if child_part_index >= child_path_len:
                break
        else:
            child_part_index = 0
            parent_part_last_match_index = -1

    return parent_part_last_match_index == len(parent_path) - 1


def delete_files(
    output_tree_queue: multiprocessing.Queue,
    input_tree: Dict,
    skipped_entries_abs_parts: List[List[str]],
    delete_skipped: bool,
    stats_deleted_ok_value: multiprocessing.Value,
    stats_deleted_error_value: multiprocessing.Value,
    control_value: multiprocessing.Value or None = None,
    logging_queue: multiprocessing.Queue or None = None,
) -> None:
    """Process body to delete files from existing backup according to input tree

    Args:
        output_files_tree_queue (multiprocessing.Queue): output tree as Queue (tree_type, filepath_rel, root_empty)
        input_tree (Dict): tree of all input files and directories
        skipped_entries_abs_parts (List[List[str]]): list of normalized absolute paths to skip or delete
        (if delete_skipped) splitted by os.path.sep
        delete_skipped (bool): True to also delete skipped files
        stats_deleted_ok_value (multiprocessing.Value): counter of total successful delete calls
        stats_deleted_error_value (multiprocessing.Value): counter of total unsuccessful delete calls
        control_value (multiprocessing.Value or None, optional): value (int) to pause / cancel process
        logging_queue (multiprocessing.Queue or None, optional): logging queue to accept logs
    """
    # Setup logging
    if logging_queue is not None:
        LoggingHandler.worker_configurer(logging_queue, False)

    # Log current PID
    current_pid = multiprocessing.current_process().pid
    if logging_queue is not None:
        logging.info(f"delete_files() with PID {current_pid} started")

    while True:
        # Check control
        if control_value is not None:
            # Get current control value
            with control_value.get_lock():
                control_value_ = control_value.value

            # Pause
            if control_value_ == PROCESS_CONTROL_PAUSE:
                if logging_queue is not None:
                    logging.info(f"delete_files() with PID {current_pid} paused")

                # Sleep loop
                while True:
                    # Retrieve updated value
                    with control_value.get_lock():
                        control_value_ = control_value.value

                    # Resume?
                    if control_value_ == PROCESS_CONTROL_WORK:
                        if logging_queue is not None:
                            logging.info(f"delete_files() with PID {current_pid} resumed")
                        break

                    # Exit?
                    elif control_value_ == PROCESS_CONTROL_EXIT:
                        if logging_queue is not None:
                            logging.info(f"delete_files() with PID {current_pid} exited upon request")
                        return

                    # Sleep some time
                    time.sleep(0.1)

            # Exit
            elif control_value_ == PROCESS_CONTROL_EXIT:
                if logging_queue is not None:
                    logging.info(f"delete_files() with PID {current_pid} exited upon request")
                return

        # Retrieve from queue and exit process on timeout
        try:
            tree_type, filepath_rel, root_empty = output_tree_queue.get(block=True, timeout=QUEUE_TIMEOUT)
        except queue.Empty:
            if logging_queue is not None:
                logging.info(f"No more files check and delete! delete_files() with PID {current_pid} exited")
            return

        try:
            # Parse data
            root = root_empty["root"]
            empty = False
            if "empty" in root_empty:
                empty = root_empty["empty"]

            # NOTE: if everything is ok, root here must be always the same an it must be backup directory

            # Convert to absolute path
            out_filepath_abs = os.path.join(root, filepath_rel)

            # Skip if not exists
            if not os.path.exists(out_filepath_abs):
                continue

            # Will be false if we don't need to delete this file / dir
            delete_flag = True

            # Try to find this path inside skipped entries
            in_skipped = False
            filepath_rel_parts = os.path.normpath(filepath_rel).split(os.path.sep)
            for skipped_entry_abs_parts in skipped_entries_abs_parts:
                if is_path_relative_to(skipped_entry_abs_parts, filepath_rel_parts):
                    in_skipped = True
                    break

            # Try to find this path inside input_tree or check if it's in skipped entries
            if in_skipped or filepath_rel in input_tree[tree_type]:
                # Decide if need to delete this file
                if not in_skipped or (in_skipped and not delete_skipped):
                    delete_flag = False

            # Skip if we don't need to delete it
            if not delete_flag:
                continue

            # Remove only link. If everything is ok, this must me 1st
            if os.path.islink(out_filepath_abs):
                os.unlink(out_filepath_abs)
                with stats_deleted_ok_value.get_lock():
                    stats_deleted_ok_value.value += 1

            # Must be 3st
            elif tree_type == "files":
                # Delete as file
                os.remove(out_filepath_abs)
                with stats_deleted_ok_value.get_lock():
                    stats_deleted_ok_value.value += 1

            # Must be 4th
            elif tree_type == "dirs":
                # Delete itself because it's empty
                if empty:
                    os.rmdir(out_filepath_abs)
                    with stats_deleted_ok_value.get_lock():
                        stats_deleted_ok_value.value += 1

                # Not empty
                else:
                    # Try to delete subdirectories and all files
                    shutil.rmtree(out_filepath_abs, ignore_errors=True)

                    # Delete itself if still exists
                    if os.path.exists(out_filepath_abs):
                        os.rmdir(out_filepath_abs)

                    # Increment stats if no error
                    with stats_deleted_ok_value.get_lock():
                        stats_deleted_ok_value.value += 1

            # "unknown" Must be 2nd
            # firstly we're trying to delete it as a file and then as a directory
            else:
                deleted = False

                # Try to delete as file
                try:
                    os.remove(out_filepath_abs)
                    deleted = True
                except Exception:
                    pass

                # Check
                if deleted:
                    with stats_deleted_ok_value.get_lock():
                        stats_deleted_ok_value.value += 1
                    continue

                # Try to delete as directory
                try:
                    # Try to delete subdirectories and all files
                    shutil.rmtree(out_filepath_abs, ignore_errors=True)

                    # Delete itself if still exists
                    if os.path.exists(out_filepath_abs):
                        os.rmdir(out_filepath_abs)
                except Exception:
                    pass

                # Check
                if deleted:
                    with stats_deleted_ok_value.get_lock():
                        stats_deleted_ok_value.value += 1

                # Cannot delete shit sh1t
                else:
                    raise Exception("Cannot be deleted either as a file or as a directory")

        # Error occurred -> log error and increment error counter
        except Exception as e:
            if logging_queue is not None:
                logging.error(f"Error deleting {filepath_rel}: {str(e)}")
            with stats_deleted_error_value.get_lock():
                stats_deleted_error_value.value += 1
