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
from pathlib import Path
import queue
import threading
import time
from typing import Dict, List, Tuple

from PyQt5 import QtCore

import ConfigManager
from DisplayablePath import DisplayablePath

from tree_parser import tree_parser, PATH_IS_FILE, PATH_IS_DIR, PATH_UNKNOWN
from checksums import parse_checksums_from_file, calculate_checksums
from delete_files import delete_files
from copy_entries import copy_entries
from _control_values import PROCESS_CONTROL_WORK, PROCESS_CONTROL_PAUSE, PROCESS_CONTROL_EXIT


# States after backup / validation finished
EXIT_CODE_SUCCESSFULLY = 0
EXIT_CODE_ERROR = 1
EXIT_CODE_CANCELED = 2


class Backupper:
    def __init__(self, config_manager: ConfigManager.ConfigManager, logging_queue: multiprocessing.Queue) -> None:
        self._config_manager = config_manager
        self._logging_queue = logging_queue

        # Set any of this flag to manage backupper process
        self.request_pause = False
        self.request_resume = False
        self.request_cancel = False

        # Current state
        self.paused = False

        # Stages counters for progress
        self._stage_current = 0
        self._stages_total = 0

        # PyQt signals
        self._progress_set_value_signal = None
        self._statusbar_show_message_signal = None
        self._backup_paused_resumed_signal = None
        self._finished_signal = None

        # Backup results
        self._stats_tree_parsed_dirs = multiprocessing.Value("i", 0)
        self._stats_tree_parsed_files = multiprocessing.Value("i", 0)
        self._stats_checksums_calculate_ok_value = multiprocessing.Value("i", 0)
        self._stats_checksums_calculate_error_value = multiprocessing.Value("i", 0)
        self._stats_deleted_ok_value = multiprocessing.Value("i", 0)
        self._stats_deleted_error_value = multiprocessing.Value("i", 0)
        self._stats_created_dirs_ok_value = multiprocessing.Value("i", 0)
        self._stats_created_dirs_error_value = multiprocessing.Value("i", 0)
        self._stats_copied_ok_value = multiprocessing.Value("i", 0)
        self._stats_copied_error_value = multiprocessing.Value("i", 0)

        # Validate results
        self._stats_validate_match = multiprocessing.Value("i", 0)
        self._stats_validate_not_match = multiprocessing.Value("i", 0)
        self._stats_validate_not_exist = multiprocessing.Value("i", 0)

    def stats_reset(self) -> None:
        """Resets all backup and validation statistics"""
        self._stats_tree_parsed_dirs.value = 0
        self._stats_tree_parsed_files.value = 0
        self._stats_checksums_calculate_ok_value.value = 0
        self._stats_checksums_calculate_error_value.value = 0
        self._stats_deleted_ok_value.value = 0
        self._stats_deleted_error_value.value = 0
        self._stats_created_dirs_ok_value.value = 0
        self._stats_created_dirs_error_value.value = 0
        self._stats_copied_ok_value.value = 0
        self._stats_copied_error_value.value = 0
        self._stats_validate_match.value = 0
        self._stats_validate_not_match.value = 0
        self._stats_validate_not_exist.value = 0

    def stats_to_str(self) -> str:
        """Parses backup statistics

        Returns:
            str: backup statistics as multiline str
        """
        stats = f"Files and directories copied: {self._stats_copied_ok_value.value}, "
        stats += f"errors: {self._stats_copied_error_value.value}\n"
        stats += f"Files and directories deleted: {self._stats_deleted_ok_value.value}, "
        stats += f"errors: {self._stats_deleted_error_value.value}\n"
        stats += f"Directories created: {self._stats_created_dirs_ok_value.value}, "
        stats += f"errors: {self._stats_created_dirs_error_value.value}\n\n"
        stats += f"(Files viewed: {self._stats_tree_parsed_files.value}, "
        stats += f"directories viewed: {self._stats_tree_parsed_dirs.value})\n"
        stats += f"(Checksums calculated: {self._stats_checksums_calculate_ok_value.value}, "
        stats += f"errors: {self._stats_checksums_calculate_error_value.value})\n\n"
        stats += "See logs for details"
        return stats

    def validation_stats_to_str(self) -> str:
        """Parses validation statistics

        Returns:
            str: validation statistics as multiline str
        """
        if (self._stats_validate_match.value + self._stats_validate_not_match.value) != 0:
            invalid_rate = (
                self._stats_validate_not_match.value
                / (self._stats_validate_match.value + self._stats_validate_not_match.value)
                * 100
            )
            stats = f"Error rate: {invalid_rate:.2f}%\n"
        else:
            stats = ""
        stats += f"Matches: {self._stats_validate_match.value}\n"
        stats += f"Mismatches: {self._stats_validate_not_match.value}\n"
        stats += f"Checksums not found: {self._stats_validate_not_exist.value}\n\n"
        stats += f"(Files viewed: {self._stats_tree_parsed_files.value}, "
        stats += f"directories viewed: {self._stats_tree_parsed_dirs.value})\n"
        stats += f"(Checksums calculated: {self._stats_checksums_calculate_ok_value.value}, "
        stats += f"errors: {self._stats_checksums_calculate_error_value.value})\n\n"
        stats += "See logs for details"
        return stats

    def stats_to_statusbar(self) -> str:
        """Parses statistics to fit statusbar

        Returns:
            str: statistics as single line str
        """
        stats = f"S: {self._stage_current} / {self._stages_total}  "

        with self._stats_tree_parsed_files.get_lock():
            stats += f"Fv: {self._stats_tree_parsed_files.value}  "
        with self._stats_tree_parsed_dirs.get_lock():
            stats += f"Dv: {self._stats_tree_parsed_dirs.value}  "

        with self._stats_checksums_calculate_ok_value.get_lock():
            stats += f"C: {self._stats_checksums_calculate_ok_value.value}, "
        with self._stats_checksums_calculate_error_value.get_lock():
            stats += f"{self._stats_checksums_calculate_error_value.value}  "

        with self._stats_copied_ok_value.get_lock():
            stats += f"FDcp: {self._stats_copied_ok_value.value}, "
        with self._stats_copied_error_value.get_lock():
            stats += f"{self._stats_copied_error_value.value}  "

        with self._stats_deleted_ok_value.get_lock():
            stats += f"FDdel: {self._stats_deleted_ok_value.value}, "
        with self._stats_deleted_error_value.get_lock():
            stats += f"{self._stats_deleted_error_value.value}  "

        with self._stats_created_dirs_ok_value.get_lock():
            stats += f"Dcr: {self._stats_created_dirs_ok_value.value}, "
        with self._stats_created_dirs_error_value.get_lock():
            stats += f"{self._stats_created_dirs_error_value.value}  "

        with self._stats_validate_match.get_lock():
            stats += f"Cvld: {self._stats_validate_match.value}, "
        with self._stats_validate_not_match.get_lock():
            stats += f"{self._stats_validate_not_match.value}, "
        with self._stats_validate_not_exist.get_lock():
            stats += f"{self._stats_validate_not_exist.value}"
        return stats

    def get_max_cpu_numbers(self) -> int:
        """Converts workload_profile to maximum number of allowed cpu cores

        Returns:
            int: allowed CPU processes
        """
        # Get workload profile
        workload_profile = self._config_manager.get_config("workload_profile").lower()

        # Get number of cpu cores
        cpu_cores = multiprocessing.cpu_count()

        # Very low -> only 1 CPU core
        if workload_profile == "very low":
            return 1

        # Low -> 25% of CPU
        if workload_profile == "low":
            return int(cpu_cores * 0.25)

        # High -> 75% of CPU
        if workload_profile == "high":
            return int(cpu_cores * 0.75)

        # Insane -> full CPU
        if workload_profile == "insane":
            return cpu_cores

        # Normal -> 50% of CPU
        else:
            return cpu_cores // 2

    def parse_input_entries(self) -> Dict:
        """Checks and parses input entries from config

        Raises:
            Exception: duplicated path / doesn't exists

        Returns:
            Dict: {"path": skip} ex. {"path_1": True, "path_2": False, ...}
        """
        input_paths = self._config_manager.get_config("input_paths")
        input_entries = {}
        for input_entry in input_paths:
            if "path" in input_entry and "skip" in input_entry:
                # Extract path
                input_path = input_entry["path"].strip()

                # Check it's length
                if not input_path or len(input_path) == 0:
                    logging.warning(f"Empty path: {input_path}")
                    continue

                # Try to normalize it
                try:
                    input_path = os.path.normpath(input_path)
                except:
                    input_path = None
                if not input_path:
                    logging.warning(f"Wrong path: {input_path}")
                    continue

                # Extract current skip flag
                skip_current = input_entry["skip"]

                # Convert to relative path (as it will be in backup dir)
                input_path_rel = os.path.relpath(input_path, os.path.dirname(input_path))

                # Prevent non-skipped duplicates
                for existing_path, existing_path_skip in input_entries.items():
                    # Convert to relative path
                    existing_path_rel = os.path.relpath(existing_path, os.path.dirname(existing_path))

                    # Check if it's the same path
                    if os.path.normpath(existing_path_rel) == os.path.normpath(input_path_rel):
                        # If existing one was not skipped and new one also now -> raise an error
                        if not existing_path_skip and not skip_current:
                            raise Exception(f"Duplicated path: {input_path}")

                # Check if it exists
                if not skip_current and not os.path.exists(input_path):
                    raise Exception(f"Path {input_path} doesn't exists and not skipped")

                # Add it to the dict
                input_entries[input_path] = input_entry["skip"]

        logging.info(f"{len(input_entries)} entries parsed")
        return input_entries

    def _update_progress_bar_status_bar(self, stage_step: int, stage_steps: int, inverted: bool = False) -> None:
        """Sets progress bar value and current stats using statusbar

        Args:
            stage_step (int): current step inside stage (starting from 0 or 1)
            stage_steps (int): total number of steps inside stage
            inverted (bool, optional): are steps progress inside stage inverted? Defaults to False.
        """
        # Exit if no progress bar updater or no stages
        if self._progress_set_value_signal is None or self._stages_total <= 0:
            return

        # Prevent division by zero
        if stage_steps == 0 or self._stages_total == 0:
            return

        # Calculate stage's step progress
        if inverted:
            steps_progress = (stage_steps - stage_step) / stage_steps
        else:
            steps_progress = stage_step / stage_steps
        steps_progress = max(min(steps_progress, 1.0), 0.0)

        # Calculate stage progress
        stage_progress_prev = (self._stage_current - 1) / self._stages_total
        stage_progress = self._stage_current / self._stages_total

        # Calculate total progress
        progress_total = steps_progress * (stage_progress - stage_progress_prev) + stage_progress_prev

        # Set value
        self._progress_set_value_signal.emit(int(progress_total * 100.0))

        # Show stats
        if self._statusbar_show_message_signal is not None:
            self._statusbar_show_message_signal.emit(self.stats_to_statusbar())

    def _exit(self, exit_status_: int) -> None:
        """Emits signals and resets requests

        Args:
            exit_status_ (int): exit code
        """
        if self._finished_signal is not None:
            self._finished_signal.emit(exit_status_)
        if self._progress_set_value_signal is not None:
            self._progress_set_value_signal.emit(100)
        self.request_pause = False
        self.request_resume = False
        self.request_cancel = False
        self.paused = False

    def _pause(self) -> bool:
        """Waits until self.request_resume or self.request_cancel

        Returns:
            bool: True if resumed, False if cancel requested during pause
        """
        # Set flags, log and emit signal
        self.request_pause = False
        self.paused = True
        logging.info("Backup paused. Waiting for resume or cancel request")
        if self._backup_paused_resumed_signal is not None:
            self._backup_paused_resumed_signal.emit(True)

        # Wait until any flag
        while not self.request_resume and not self.request_cancel:
            time.sleep(0.01)

        # Cancel requested
        if self.request_cancel:
            logging.info("Cancel requested during pause")
            return False

        # Resume requested
        else:
            self.request_resume = False
            self.paused = False
            logging.info("Backup resumed")
            if self._backup_paused_resumed_signal is not None:
                self._backup_paused_resumed_signal.emit(False)
            return True

    def _process_controller(
        self,
        processes: List,
        control_value: multiprocessing.Value,
        filler_exit_flag: Dict or None = None,
        process_output_queue: multiprocessing.Queue or None = None,
    ) -> int:
        """Checks self.request_cancel and self.request_pause and calls _cancel and _pause and kills all processes

        Args:
            processes (List): list of processes
            control_value (multiprocessing.Value): Value to communicate with processes
            filler_exit_flag (Dict or None, optional): dict to communicate with filler thread
            process_output_queue (multiprocessing.Queue or None, optional): output from processes to clean it on exit

        Returns:
            int: >= 0 in case of exit or error
        """

        def _process_killer(
            processes_: List,
            control_value_: multiprocessing.Value,
            process_output_queue_: multiprocessing.Queue or None,
        ) -> None:
            """Kills active processes and cleans queue if needed
            TODO: This DEFINITELY needs refactoring!

            Args:
                processes_ (List): list of processes
                control_value_ (multiprocessing.Value): Value to communicate with processes
                process_output_queue_ (multiprocessing.Queue or None, optional): output from processes
            """

            def __queue_cleaner(process_output_queue__: multiprocessing.Queue or None) -> None:
                """Clears queue

                Args:
                    process_output_queue__ (multiprocessing.Queue or None): output from processes
                """
                if process_output_queue__ is not None:
                    while True:
                        try:
                            process_output_queue__.get_nowait()
                        except queue.Empty:
                            break

            # Request exit from all processes
            with control_value_.get_lock():
                logging.info("Requesting processes exit")
                control_value_.value = PROCESS_CONTROL_EXIT

            # Clear queue and give them some time
            sleep_timer_ = time.time()
            while time.time() - sleep_timer_ < 1:
                __queue_cleaner(process_output_queue_)
                time.sleep(0.1)

            # Check and kill them until they finished
            while True:
                killed_all_ = True
                for process_ in processes_:
                    # Kill if still alive
                    if process_ is not None and process_.is_alive():
                        killed_all_ = False
                        try:
                            logging.info(f"Killing {process_.pid}")
                            process_.kill()
                            time.sleep(0.1)
                        except Exception as e:
                            logging.warning(f"Error killing process with PID {process_.pid}: {str(e)}")

                # Exit when no alive process remained
                if killed_all_:
                    break

                # Clear queue and give them some time
                sleep_timer_ = time.time()
                while time.time() - sleep_timer_ < 1:
                    __queue_cleaner(process_output_queue_)
                    time.sleep(0.1)

            # Done killing
            logging.info("All process finished!")

        # Check cancel flag
        if self.request_cancel:
            logging.warning("Received cancel request!")
            # Request filler exit
            if filler_exit_flag is not None:
                filler_exit_flag["exit"] = True

            # Kill all processes and exit
            _process_killer(processes, control_value, process_output_queue)
            self._exit(EXIT_CODE_CANCELED)
            return EXIT_CODE_CANCELED

        # Check pause flag
        if self.request_pause:
            logging.warning("Received pause request!")
            # Request pause
            with control_value.get_lock():
                control_value.value = PROCESS_CONTROL_PAUSE

            # Cancel during pause
            if not self._pause():
                # Request filler exit
                if filler_exit_flag is not None:
                    filler_exit_flag["exit"] = True

                # Kill all processes and exit
                _process_killer(processes, control_value, process_output_queue)
                self._exit(EXIT_CODE_CANCELED)
                return EXIT_CODE_CANCELED

            # Resume processes
            with control_value.get_lock():
                control_value.value = PROCESS_CONTROL_WORK

        # No exit requested
        return -1

    def _generate_tree(
        self, entries: Dict, root_relative_to_dirname: bool = False, ignore_filepaths_abs: List or None = None
    ) -> Tuple[Dict, List[str]] or int:
        """Parses entries and generates dict of files and dirs with the following structure using multiprocessing:
        ({
            "files": {
                "file_1_relative_to_root_path": {
                    "root": "file_1_root_directory"
                },
                "file_2_relative_to_root_path": {
                    "root": "file_2_root_directory"
                }
            },
            "dirs": {
                "dir_1_relative_to_root_path": {
                    "root": "dir_1_root_directory"
                    "empty": False
                },
                "dir_2_relative_to_root_path": {
                    "root": "dir_2_root_directory"
                    "empty": True
                }
            },
            "unknown": {
                "this_path_is_wrong_or_does_not_exists": {
                    "root": "path_1_root_directory"
                }
            }
        }, ["skipped_abs_path_1", "skipped_abs_path_2"])

        Args:
            entries (Dict): {"path": skip} ex. {"path_1": True, "path_2": False, ...}
            root_relative_to_dirname (bool): True to calculate path relative to dirname(root_dir) instead of root_dir
            ignore_filepaths_abs (List, optional): list of absolute filepaths to exclude from tree. Defaults to None.

        Returns:
            Tuple[Dict, List[str]] or int: parsed tree and skipped entries or exit status in case of cancel
        """
        logging.info(f"Generating tree for {len(entries)} entries")

        # Create recursion queue (and add each root dir after that)
        directories_to_parse_queue = multiprocessing.Queue(-1)

        # Create output queue (and add each file, dir and unknown path after that)
        parsed_queue = multiprocessing.Queue(-1)

        # Skipped entries as list of absolute paths
        skipped_entries_abs = []

        # Add each entry
        for path_abs, skip in entries.items():
            # Ignore path
            if ignore_filepaths_abs and path_abs in ignore_filepaths_abs:
                continue

            # Skipped entry
            if skip:
                skipped_entries_abs.append(path_abs)
                continue

            # Extract root directory
            root_dir = os.path.dirname(path_abs) if root_relative_to_dirname else path_abs

            # Get path type
            path_type = (
                PATH_IS_FILE
                if os.path.isfile(path_abs)
                else (PATH_IS_DIR if os.path.isdir(path_abs) else PATH_UNKNOWN)
            )

            # Add to the output queue
            # (relative path, root dir path, PATH_..., is empty directory)
            parsed_queue.put((os.path.relpath(path_abs, root_dir), root_dir, path_type, False))

            # Parse directories
            if path_type == PATH_IS_DIR:
                # Extract parent directory
                parent_dir = os.path.relpath(path_abs, root_dir) if root_relative_to_dirname else ""

                # Add to the recursion queue (parent dir, abs root path)
                if not skip:
                    directories_to_parse_queue.put((parent_dir, root_dir))

        # Create control Value for pause and cancel
        control_value = multiprocessing.Value("i", PROCESS_CONTROL_WORK)

        # Output data
        tree = {"files": {}, "dirs": {}, "unknown": {}}

        # Start processes
        processes_num = self.get_max_cpu_numbers()
        processes = []
        for i in range(processes_num):
            process = multiprocessing.Process(
                target=tree_parser,
                args=(
                    directories_to_parse_queue,
                    parsed_queue,
                    skipped_entries_abs,
                    self._config_manager.get_config("follow_symlinks"),
                    self._stats_tree_parsed_dirs,
                    self._stats_tree_parsed_files,
                    control_value,
                    self._logging_queue,
                ),
            )
            logging.info(f"Starting process {i + 1} / {processes_num}")
            process.start()
            processes.append(process)

        # Loop until they all finished
        while True:
            # Check processes
            finished = True
            for process in processes:
                if process.is_alive():
                    finished = False
                    break

            # Retrieve data from parsed_queue
            while not parsed_queue.empty():
                try:
                    # Get data
                    path_rel, root_dir, path_type, is_empty = parsed_queue.get(block=True, timeout=0.1)

                    # Convert to absolute path
                    path_abs = os.path.join(root_dir, path_rel)

                    # Check if we need to exclude it
                    if ignore_filepaths_abs and path_abs in ignore_filepaths_abs:
                        continue

                    # Put into tree
                    if path_type is PATH_IS_FILE:
                        tree["files"][path_rel] = {"root": root_dir}
                    elif path_type is PATH_IS_DIR:
                        tree["dirs"][path_rel] = {"root": root_dir, "empty": is_empty}
                    else:
                        tree["unknown"][path_rel] = {"root": root_dir}
                except queue.Empty:
                    break

                # Handle pause and cancel requests even inside this loop
                exit_code = self._process_controller(processes, control_value, process_output_queue=parsed_queue)
                if exit_code >= 0:
                    return exit_code

            # Done!
            if finished:
                logging.info("Tree generation finished!")
                break

            # Update statusbar
            if self._statusbar_show_message_signal is not None:
                self._statusbar_show_message_signal.emit(self.stats_to_statusbar())

            # Handle pause and cancel requests
            exit_code = self._process_controller(processes, control_value, process_output_queue=parsed_queue)
            if exit_code >= 0:
                return exit_code

            # Prevent overloading
            time.sleep(0.1)

        # Return generated tree
        return tree, skipped_entries_abs

    def _calculate_checksum(
        self,
        checksum_out_file: str or None,
        exclude_checksums: Dict or None,
        checksum_alg: str,
        files_tree: Dict,
        output_as_absolute_paths: bool,
    ) -> Dict or int:
        """Calculates checksum of local_files_queue using multiprocessing

        Args:
            checksum_out_file (str or None): file to append (a+) checksums or None to don't write to it
            exclude_checksums (Dist): exclude from calculating checksum:
            {
                "file_1_to_exclude_relative_to_root_path": {
                    "root": "file_1_root_directory",
                    "checksum": "file_1_checksum"
                },
                ...
            }
            checksum_alg (str): MD5 / SHA256 / SHA512
            files_tree (Dict): dict of "files" of tree to calculate checksum of:
            {
                "file_1_relative_to_root_path": {
                    "root": "file_1_root_directory"
                },
                "file_2_relative_to_root_path": {
                    "root": "file_2_root_directory"
                }
            }
            output_as_absolute_paths (bool): True to write filepaths as absolute paths instead of relative to root dir,
            only matters if checksum_out_file is specified

        Returns:
            Dict or int: checksums as dictionary or exit status in case of cancel
            Output dict will have this format:
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
        """

        def _files_tree_queue_filler(
            files_tree_: Dict,
            filepaths_queue_: multiprocessing.Queue,
            exclude_checksums_: Dict or None,
            exit_flag_: Dict,
        ) -> None:
            """Thread body that dynamically puts files from files_tree_ into filepaths_queue_

            Args:
                files_tree_ (Dict): dict of "files" of tree to calculate checksum
                filepaths_queue_ (multiprocessing.Queue): queue of file paths as tuples (root dir, local path)
                exclude_checksums (Dist): exclude from calculating checksum
                exit_flag_ (Dict): {"exit": False}
            """
            logging.info("Filler thread started")
            update_progress_timer_ = time.time()
            progress_counter = 0
            skipped_counter = 0
            size_total_ = len(files_tree_)
            for path_relative_, root_ in files_tree_.items():
                # Check if we need to exit
                if exit_flag_["exit"]:
                    break

                # Extract root dir
                root_ = root_["root"]

                # Check if we need to exclude it
                if exclude_checksums_ is not None and path_relative_ in exclude_checksums_:
                    skipped_counter += 1
                    continue

                # Put in the queue as (relative path, root dir)
                filepaths_queue_.put((path_relative_, root_), block=True)

                # Increment items counter
                progress_counter += 1

                # Update progress every 100ms
                if time.time() - update_progress_timer_ >= 0.1:
                    self._update_progress_bar_status_bar(progress_counter, size_total_)

            # Done
            logging.info(f"Filler thread finished. Skipped {skipped_counter} checksums")

        # Create control Value for pause and cancel
        control_value = multiprocessing.Value("i", PROCESS_CONTROL_WORK)

        # Create Lock to prevent writing to the file at the same time
        if checksum_out_file is not None:
            checksum_out_file_lock = multiprocessing.Manager().Lock()
        else:
            checksum_out_file_lock = None

        # Create queue if we need to return checksums as dictionary instead of writing them to the file
        if checksum_out_file is None:
            checksum_out_queue = multiprocessing.Queue(-1)
        else:
            checksum_out_queue = None

        # Calculate started size (for processes_num)
        size_started = len(files_tree)
        skip_str = (
            " and skipping some of them" if (exclude_checksums is not None and len(exclude_checksums) != 0) else ""
        )
        logging.info(f"Calculating checksums for {size_started} files{skip_str}")

        # Calculate number of processes
        processes_num = min(size_started, self.get_max_cpu_numbers())

        # Calculate filepaths_queue size as 10 files per process (seems enough for me ¯\_(ツ)_/¯)
        filepaths_queue_size = processes_num * 10

        # Generate input queue
        filepaths_queue = multiprocessing.Queue(filepaths_queue_size)

        # Start filler as thread
        filler_exit_flag = {"exit": False}
        logging.info("Starting filler thread")
        threading.Thread(
            target=_files_tree_queue_filler,
            args=(
                files_tree,
                filepaths_queue,
                exclude_checksums,
                filler_exit_flag,
            ),
        ).start()

        # Dictionary to store calculated checksums
        checksums = {}

        # Start processes
        processes = []
        for i in range(processes_num):
            process = multiprocessing.Process(
                target=calculate_checksums,
                args=(
                    checksum_out_file,
                    checksum_out_queue,
                    checksum_alg,
                    filepaths_queue,
                    output_as_absolute_paths,
                    self._stats_checksums_calculate_ok_value,
                    self._stats_checksums_calculate_error_value,
                    control_value,
                    checksum_out_file_lock,
                    self._logging_queue,
                ),
            )
            logging.info(f"Starting process {i + 1} / {processes_num}")
            process.start()
            processes.append(process)

        # Loop until they all finished
        while True:
            # Check processes
            finished = True
            for process in processes:
                if process.is_alive():
                    finished = False
                    break

            # Retrieve data from queue
            if checksum_out_queue is not None:
                while not checksum_out_queue.empty():
                    try:
                        filepath_rel, filepath_root_dir, checksum = checksum_out_queue.get(block=True, timeout=0.1)
                        checksums[filepath_rel] = {"root": filepath_root_dir, "checksum": checksum}
                    except queue.Empty:
                        break

                    # Handle pause and cancel requests even inside this loop
                    exit_code = self._process_controller(
                        processes, control_value, filler_exit_flag, checksum_out_queue
                    )
                    if exit_code >= 0:
                        return exit_code

            # Done!
            if finished:
                filler_exit_flag["exit"] = True
                logging.info("Checksum calculating finished!")
                break

            # Handle pause and cancel requests
            exit_code = self._process_controller(processes, control_value, filler_exit_flag, checksum_out_queue)
            if exit_code >= 0:
                return exit_code

            # Prevent overloading
            time.sleep(0.1)

        # Finished OK
        return checksums

    def _delete_files(self, input_tree: Dict, output_tree: Dict, skipped_entries_abs: List[str]) -> int:
        """Deletes existing files from output dir according to input data

        Args:
            input_tree (Dict): all input files and directories
            output_tree (Dict): all output files and directories
            skipped_entries_abs (List[str]): list of normalized absolute paths to skip or delete (if delete_skipped)

        Returns:
            int: >=0 in case of exit
        """

        def _output_tree_queue_filler(
            output_tree_: Dict,
            output_tree_queue_: multiprocessing.Queue,
            exit_flag_: Dict,
        ) -> None:
            """Thread body that dynamically puts files from files_tree_ into filepaths_queue_

            Args:
                output_tree_ (Dict): tree of output files and directories
                output_tree_queue_ (multiprocessing.Queue): queue of of tree entries
                (tree_type, filepath_rel, root_empty)
                exit_flag_ (Dict): {"exit": False}
            """
            logging.info("Filler thread started")
            update_progress_timer_ = time.time()
            progress_counter = 0
            size_total_ = len(output_tree_["files"]) + len(output_tree_["dirs"]) + len(output_tree_["unknown"])
            for tree_type_, local_tree_ in output_tree_.items():
                for path_relative_, root_and_empty_ in local_tree_.items():
                    # Check if we need to exit
                    if exit_flag_["exit"]:
                        break

                    # Put in the queue as (tree_type, filepath_rel, root_empty)
                    output_tree_queue_.put((tree_type_, path_relative_, root_and_empty_), block=True)

                    # Increment items counter
                    progress_counter += 1

                    # Update progress every 100ms
                    if time.time() - update_progress_timer_ >= 0.1:
                        self._update_progress_bar_status_bar(progress_counter, size_total_)

            # Done
            logging.info("Filler thread finished")

        # Parse skipped entries
        skipped_entries_abs_parts = [
            os.path.normpath(skipped_entry_abs).split(os.path.sep) for skipped_entry_abs in skipped_entries_abs
        ]

        # Create control Value for pause and cancel
        control_value = multiprocessing.Value("i", PROCESS_CONTROL_WORK)

        # Calculate number of processes
        processes_num = min(len(output_tree["files"]), self.get_max_cpu_numbers())

        # Calculate output_tree_queue size as 10 files per process (seems enough for me ¯\_(ツ)_/¯)
        output_tree_queue_size = processes_num * 10

        # Generate input queue
        output_tree_queue = multiprocessing.Queue(output_tree_queue_size)

        # Start filler as thread
        filler_exit_flag = {"exit": False}
        logging.info("Starting filler thread")
        threading.Thread(
            target=_output_tree_queue_filler,
            args=(
                output_tree,
                output_tree_queue,
                filler_exit_flag,
            ),
        ).start()

        # Start processes
        processes = []
        for i in range(processes_num):
            process = multiprocessing.Process(
                target=delete_files,
                args=(
                    output_tree_queue,
                    input_tree,
                    skipped_entries_abs_parts,
                    self._config_manager.get_config("delete_skipped"),
                    self._stats_deleted_ok_value,
                    self._stats_deleted_error_value,
                    control_value,
                    self._logging_queue,
                ),
            )
            logging.info(f"Starting process {i + 1} / {processes_num}")
            process.start()
            processes.append(process)

        # Wait until they all finished
        while True:
            # Check processes
            finished = True
            for process in processes:
                if process.is_alive():
                    finished = False
                    break

            # Done!
            if finished:
                logging.info("Files deleting processes finished!")
                break

            # Handle pause and cancel requests
            exit_code = self._process_controller(processes, control_value, filler_exit_flag)
            if exit_code >= 0:
                return exit_code

            # Prevent overloading
            time.sleep(0.1)

        # Finished OK
        return -1

    def _copy_entries(
        self,
        input_files_tree: Dict,
        checksums_input: Dict,
        checksums_output: Dict,
        output_dir: str,
    ) -> int:
        """Copies input entries to output directory based on checksums

        Args:
            input_files_tree (Dict): "files" dictionary of all input files (see docs above for more info)
            checksums_input (Dict): checksums of input files
            checksums_output (Dict): checksums of output files
            output_dir (str): path to the output (backup) directory

        Returns:
            int: >=0 in case of exit
        """

        def _files_tree_queue_filler(
            files_tree_: Dict,
            filepaths_queue_: multiprocessing.Queue,
            exit_flag_: Dict,
        ) -> None:
            """Thread body that dynamically puts files from files_tree_ into filepaths_queue_

            Args:
                files_tree_ (Dict): dict of "files" of tree to calculate checksum
                filepaths_queue_ (multiprocessing.Queue): queue of file paths as tuples (root dir, local path)
                exit_flag_ (Dict): {"exit": False}
            """
            logging.info("Filler thread started")
            update_progress_timer_ = time.time()
            progress_counter = 0
            size_total_ = len(files_tree_)
            for path_relative_, root_ in files_tree_.items():
                # Check if we need to exit
                if exit_flag_["exit"]:
                    break

                # Extract root dir
                root_ = root_["root"]

                # Put in the queue as (relative path, root dir)
                filepaths_queue_.put((path_relative_, root_), block=True)

                # Increment items counter
                progress_counter += 1

                # Update progress every 100ms
                if time.time() - update_progress_timer_ >= 0.1:
                    self._update_progress_bar_status_bar(progress_counter, size_total_)

            # Done
            logging.info("Filler thread finished")

        # Create control Value for pause and cancel
        control_value = multiprocessing.Value("i", PROCESS_CONTROL_WORK)

        # Calculate number of processes
        processes_num = min(len(input_files_tree), self.get_max_cpu_numbers())

        # Calculate filepaths_queue size as 10 files per process (seems enough for me ¯\_(ツ)_/¯)
        filepaths_queue_size = processes_num * 10

        # Generate input queue
        filepaths_queue = multiprocessing.Queue(filepaths_queue_size)

        # Start filler as thread
        filler_exit_flag = {"exit": False}
        logging.info("Starting filler thread")
        threading.Thread(
            target=_files_tree_queue_filler,
            args=(
                input_files_tree,
                filepaths_queue,
                filler_exit_flag,
            ),
        ).start()

        # Start processes
        processes = []
        for i in range(processes_num):
            process = multiprocessing.Process(
                target=copy_entries,
                args=(
                    filepaths_queue,
                    checksums_input,
                    checksums_output,
                    output_dir,
                    self._stats_copied_ok_value,
                    self._stats_copied_error_value,
                    self._stats_created_dirs_ok_value,
                    self._stats_created_dirs_error_value,
                    control_value,
                    self._logging_queue,
                ),
            )
            logging.info(f"Starting process {i + 1} / {processes_num}")
            process.start()
            processes.append(process)

        # Wait until they all finished
        while True:
            # Check processes
            finished = True
            for process in processes:
                if process.is_alive():
                    finished = False
                    break

            # Done!
            if finished:
                logging.info("Files copying finished!")
                break

            # Handle pause and cancel requests
            exit_code = self._process_controller(processes, control_value, filler_exit_flag)
            if exit_code >= 0:
                return exit_code

            # Prevent overloading
            time.sleep(0.1)

        # Finished OK
        return -1

    def start_backup(
        self,
        input_entries: Dict,
        output_dir: str,
        progress_set_value_signal: QtCore.pyqtSignal or None = None,
        statusbar_show_message_signal: QtCore.pyqtSignal or None = None,
        backup_paused_resumed_signal: QtCore.pyqtSignal or None = None,
        finished_signal: QtCore.pyqtSignal or None = None,
    ) -> int:
        """Start backup (this is the heart of app) (blocking)

        Args:
            input_entries (Dict): parsed valid input entries as dictionary {"path": skip} ex. {"path_1": False, ...}
            output_dir (str): backup directory
            progress_set_value_signal (QtCore.pyqtSignal or None): PyQt signal (int) to update progress bar
            statusbar_show_message_signal (QtCore.pyqtSignal or None): PyQt signal (int) to update status bar
            backup_paused_resumed_signal (QtCore.pyqtSignal or None): PyQt signal (bool) for pause callback
            finished_signal (QtCore.pyqtSignal or None): PyQt signal (int) for exit callback

        Returns:
            int: EXIT_CODE_... code
        """
        # Update signals
        self._progress_set_value_signal = progress_set_value_signal
        self._statusbar_show_message_signal = statusbar_show_message_signal
        self._backup_paused_resumed_signal = backup_paused_resumed_signal
        self._finished_signal = finished_signal

        # Clear flags
        self.request_pause = False
        self.request_resume = False
        self.request_cancel = False
        self.paused = False

        # Reset stats
        self.stats_reset()

        # Initial exit status (default one)
        exit_status = EXIT_CODE_SUCCESSFULLY

        try:
            # Calculate total number of stages and reset current stage
            self._stages_total = 5
            if self._config_manager.get_config("delete_data"):
                self._stages_total += 1
            if self._config_manager.get_config("create_empty_dirs"):
                self._stages_total += 1
            if self._config_manager.get_config("generate_tree"):
                self._stages_total += 1
            self._stage_current = 0

            ####################################
            # STAGE 1: Prepare and parse files #
            ####################################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Preparing and parsing files")

            # Reset progress
            if progress_set_value_signal is not None:
                progress_set_value_signal.emit(0)

            # Create output directories
            if not os.path.exists(output_dir):
                logging.info(f"Creating {output_dir} directory")
                os.makedirs(output_dir)

            # Update progress bar (no real progress here)
            self._update_progress_bar_status_bar(10, 100)

            # Parse input_entries and extract all files and dirs
            logging.info("Generating input entries tree")
            input_tree = self._generate_tree(input_entries, root_relative_to_dirname=True)

            # Exit ?
            if isinstance(input_tree, int):
                return input_tree

            # Split tuple
            input_tree, skipped_entries_abs = input_tree

            # Log
            logging.info(
                f"Files: {len(input_tree['files'])}, "
                f"dirs: {len(input_tree['dirs'])}, "
                f"unknown paths: {len(input_tree['unknown'])}"
            )

            # Update progress bar (no real progress here)
            self._update_progress_bar_status_bar(50, 100)

            # Extract all existing files inside backup (output_directory)
            logging.info("Generating output (existing files) tree")
            output_tree = self._generate_tree({output_dir: False}, ignore_filepaths_abs=[output_dir])

            # Exit ?
            if isinstance(output_tree, int):
                return output_tree

            # Split tuple
            output_tree, _ = output_tree

            # Log
            logging.info(
                f"Files: {len(output_tree['files'])}, "
                f"dirs: {len(output_tree['dirs'])}, "
                f"unknown paths: {len(output_tree['unknown'])}"
            )

            # Update progress bar (no real progress here)
            self._update_progress_bar_status_bar(100, 100)

            #############################################
            # STAGE 2: Generate input entries checksums #
            #############################################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Calculating checksums for input files")

            # Extract checksum algorithm name from config
            checksum_alg = self._config_manager.get_config("checksum_alg")

            # Calculate checksum using absolute file locations
            checksums_input = self._calculate_checksum(
                None,
                None,
                checksum_alg,
                input_tree["files"],
                True,
            )

            # Exit ?
            if isinstance(checksums_input, int):
                return checksums_input

            ############################################
            # STAGE 3: Generate output files checksums #
            ############################################
            self._stage_current += 1
            logging.info(
                f"STAGE {self._stage_current} / {self._stages_total}: "
                "Calculating checksums of existing files inside backup"
            )

            # Calculate checksums output file
            checksum_file_out = os.path.join(output_dir, f"checksums.{checksum_alg.lower()}")

            # Parse from file
            checksums_output_parsed = {}
            if not self._config_manager.get_config("recalculate_checksum"):
                checksums_output_parsed = parse_checksums_from_file(checksum_file_out, output_dir, checksum_alg)

            # Calculate checksum using relative file locations
            checksums_output = self._calculate_checksum(
                None,
                checksums_output_parsed,
                checksum_alg,
                output_tree["files"],
                False,
            )

            # Exit ?
            if isinstance(checksums_output, int):
                return checksums_output

            # Merge
            logging.info(
                f"Merging {len(checksums_output)} calculated checksums with {len(checksums_output_parsed)} parsed"
            )
            checksums_output.update(checksums_output_parsed)

            ################################################################
            # STAGE 4 (optional): Delete files and directories from backup #
            ################################################################
            if self._config_manager.get_config("delete_data"):
                # Delete files
                self._stage_current += 1
                logging.info(
                    f"STAGE {self._stage_current} / {self._stages_total}: "
                    "Deleting files from backup according to input"
                )

                # Delete files
                delete_files_exit_code = self._delete_files(input_tree, output_tree, skipped_entries_abs)

                # Exit ?
                if delete_files_exit_code >= 0:
                    return delete_files_exit_code

            ##############################################################
            # STAGE 5 (optional): Create empty directories inside backup #
            ##############################################################
            if self._config_manager.get_config("create_empty_dirs"):
                self._stage_current += 1
                logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Creating empty directories")

                # Do everything in main process because it's not that hard
                update_progress_timer_ = time.time()
                progress_counter = 0
                empty_dirs_created_counter = 0
                size_total_ = len(input_tree["dirs"])
                for path_relative, root_and_empty in input_tree["dirs"].items():
                    try:
                        # Parse data
                        empty = root_and_empty["empty"]

                        # Ignore if it's not empty
                        if not empty:
                            continue

                        # Convert to absolute path combining with output_dir
                        out_path_abs = os.path.join(output_dir, path_relative)

                        # Generate directories if they're not exist
                        if not os.path.exists(out_path_abs):
                            os.makedirs(out_path_abs)
                            with self._stats_created_dirs_ok_value.get_lock():
                                self._stats_created_dirs_ok_value.value += 1
                            empty_dirs_created_counter += 1

                        # Check cancel flag
                        if self.request_cancel:
                            self._exit(EXIT_CODE_CANCELED)
                            return EXIT_CODE_CANCELED

                        # Check pause flag
                        if self.request_pause:
                            if not self._pause():
                                self._exit(EXIT_CODE_CANCELED)
                                return EXIT_CODE_CANCELED

                        # Increment items counter
                        progress_counter += 1

                        # Update progress every 100ms
                        if time.time() - update_progress_timer_ >= 0.1:
                            self._update_progress_bar_status_bar(progress_counter, size_total_)

                    # Error occurred -> log error and increment error counter
                    except Exception as e:
                        logging.error(f"Error creating directory {path_relative}: {str(e)}")
                        with self._stats_created_dirs_error_value.get_lock():
                            self._stats_created_dirs_error_value.value += 1

                # Log
                logging.info(f"Created {empty_dirs_created_counter} empty directories")

            ################################################
            # STAGE 6: Copy files (and create directories) #
            ################################################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Copying files")

            copy_entries_exit_code = self._copy_entries(
                input_tree["files"],
                checksums_input,
                checksums_output,
                output_dir,
            )

            # Exit ?
            if copy_entries_exit_code >= 0:
                return copy_entries_exit_code

            ############################################
            # STAGE 7: Calculate final backup checksum #
            ############################################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Calculating final checksums")

            # Delete output checksum file
            if os.path.exists(checksum_file_out):
                logging.info(f"Deleting {checksum_file_out}")
                os.remove(checksum_file_out)

            # Recalculate
            if self._config_manager.get_config("recalculate_checksum"):
                # Extract all existing files / directories inside backup (output_directory) again
                logging.info("Generating output (existing files) tree again")
                output_tree = self._generate_tree({output_dir: False}, ignore_filepaths_abs=[output_dir])

                # Exit ?
                if isinstance(output_tree, int):
                    return output_tree

                # Split tuple
                output_tree, _ = output_tree

                # Log
                logging.info(
                    f"Files: {len(input_tree['files'])}, "
                    f"dirs: {len(input_tree['dirs'])}, "
                    f"unknown paths: {len(input_tree['unknown'])}"
                )

                # Calculate output checksums again
                del checksums_output
                checksums_output = self._calculate_checksum(
                    None,
                    None,
                    checksum_alg,
                    output_tree["files"],
                    False,
                )

                # Exit ?
                if isinstance(checksums_output, int):
                    return checksums_output

            # Reuse -> merge with input checksums
            else:
                logging.info("Merging checksums")
                checksums_output.update(checksums_input)

            # Delete checksums file
            if os.path.exists(checksum_file_out):
                logging.info(f"Deleting {checksum_file_out} file")
                os.remove(checksum_file_out)

            # Write to file
            logging.info(f"Writing checksums to the {checksum_file_out}")
            with open(checksum_file_out, "w+", encoding="utf8") as checksum_file_out_stream:
                for path_relative, root_and_checksum in checksums_output.items():
                    # Extract checksum
                    checksum = root_and_checksum["checksum"]

                    # Write to the file
                    checksum_file_out_stream.write(f"{checksum} *{path_relative}\n")

            #########################################
            # STAGE 8 (optional): Generate tree.txt #
            #########################################
            if self._config_manager.get_config("generate_tree"):
                self._stage_current += 1
                logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Creating backup tree")

                # Generate file
                tree_file = os.path.join(output_dir, "tree.txt")

                # Delete it if exists
                if os.path.exists(tree_file):
                    logging.info(f"Deleting {tree_file} file")
                    os.remove(tree_file)

                # Write tree to file
                update_progress_timer_ = time.time()
                progress_counter = 0
                size_total_ = len(output_tree["files"]) + len(output_tree["dirs"])
                with open(tree_file, "w+", encoding="utf8") as tree_file_stream:
                    for path in DisplayablePath.make_tree(
                        Path(output_dir), follow_symlinks=self._config_manager.get_config("follow_symlinks")
                    ):
                        tree_file_stream.write(path.displayable() + "\n")

                        # Check cancel flag
                        if self.request_cancel:
                            self._exit(EXIT_CODE_CANCELED)
                            return EXIT_CODE_CANCELED

                        # Check pause flag
                        if self.request_pause:
                            if not self._pause():
                                self._exit(EXIT_CODE_CANCELED)
                                return EXIT_CODE_CANCELED

                        # Increment items counter
                        progress_counter += 1

                        # Update progress every 100ms
                        if time.time() - update_progress_timer_ >= 0.1:
                            self._update_progress_bar_status_bar(progress_counter, size_total_)

            # Update statusbar
            if self._statusbar_show_message_signal is not None:
                self._statusbar_show_message_signal.emit(self.stats_to_statusbar())

            # All stages finished
            logging.info("Meow! ฅ^•ﻌ•^ฅ Backup finished!")

        # Finished with error
        except Exception as e:
            logging.error("Error backing up data!", exc_info=e)
            exit_status = EXIT_CODE_ERROR

        # Finished
        self._exit(exit_status)
        return exit_status

    def validate(
        self,
        output_dir: str,
        progress_set_value_signal: QtCore.pyqtSignal or None = None,
        statusbar_show_message_signal: QtCore.pyqtSignal or None = None,
        finished_signal: QtCore.pyqtSignal or None = None,
    ) -> int:
        """Validates existing backup without copying / writing / deleting any files by comparing existing checksums

        Args:
            output_dir (str): absolute path of directory with backup
            progress_set_value_signal (QtCore.pyqtSignal or None, optional): PyQt signal (int) to update progress bar
            statusbar_show_message_signal (QtCore.pyqtSignal or None): PyQt signal (int) to update status bar
            finished_signal (QtCore.pyqtSignal or None, optional): PyQt signal (int) for exit callback

        Returns:
            int: EXIT_CODE_... code
        """
        # Update signals
        self._progress_set_value_signal = progress_set_value_signal
        self._statusbar_show_message_signal = statusbar_show_message_signal
        self._backup_paused_resumed_signal = None
        self._finished_signal = finished_signal

        # Clear flags
        self.request_pause = False
        self.request_resume = False
        self.request_cancel = False
        self.paused = False

        # Reset stats
        self.stats_reset()

        # Initial exit status (default one)
        exit_status = EXIT_CODE_SUCCESSFULLY

        # Initialize stages counters
        self._stages_total = 4
        self._stage_current = 0

        # Extract from config
        checksum_alg = self._config_manager.get_config("checksum_alg")

        try:
            #################################
            # STAGE 1: Parse checksums file #
            #################################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Parsing checksums file")

            # Build filepath and check if it exists
            checksum_file_out = os.path.join(output_dir, f"checksums.{checksum_alg.lower()}")
            if not os.path.exists(checksum_file_out):
                raise Exception(f"File {checksum_file_out} doesn't exist!")

            # Parse it
            checksums_parsed = parse_checksums_from_file(checksum_file_out, output_dir, checksum_alg)

            ########################
            # STAGE 2: Parse files #
            ########################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Parsing backup files and directories")

            # Extract all existing files inside backup (output_directory)
            logging.info("Generating output (existing files) tree")
            output_tree = self._generate_tree({output_dir: False}, ignore_filepaths_abs=[output_dir])

            # Exit ?
            if isinstance(output_tree, int):
                return output_tree

            # Split tuple
            output_tree, _ = output_tree

            # Log
            logging.info(
                f"Files: {len(output_tree['files'])}, "
                f"dirs: {len(output_tree['dirs'])}, "
                f"unknown paths: {len(output_tree['unknown'])}"
            )

            ################################
            # STAGE 3: Calculate checksums #
            ################################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Calculating checksums")

            # Calculate checksum using relative file locations
            checksums_output = self._calculate_checksum(
                None,
                None,
                checksum_alg,
                output_tree["files"],
                False,
            )

            # Exit ?
            if isinstance(checksums_output, int):
                return checksums_output

            ##############################
            # STAGE 4: Compare checksums #
            ##############################
            self._stage_current += 1
            logging.info(f"STAGE {self._stage_current} / {self._stages_total}: Comparing checksums")

            # Compare newly calculated checksums against parsed
            update_progress_timer_ = time.time()
            progress_counter = 0
            size_total_ = len(checksums_output)
            for filepath_rel, root_and_checksum in checksums_output.items():
                # Extract checksum
                checksum = root_and_checksum["checksum"]

                # Convert to absolute path
                filepath_abs = os.path.join(output_dir, filepath_rel)

                # Ignore checksums file itself
                if (
                    os.path.exists(filepath_abs)
                    and os.path.exists(checksum_file_out)
                    and os.path.samefile(filepath_abs, checksum_file_out)
                ):
                    continue

                # Try to extract checksum from parsed file
                checksum_old = None
                if filepath_rel in checksums_parsed:
                    checksum_old = checksums_parsed[filepath_rel]["checksum"]

                # Doesn't exists
                if not checksum_old:
                    with self._stats_validate_not_exist.get_lock():
                        self._stats_validate_not_exist.value += 1
                    continue

                # Compare
                if checksum_old == checksum:
                    with self._stats_validate_match.get_lock():
                        self._stats_validate_match.value += 1

                # Not matched!
                else:
                    logging.warning(
                        f"Actual checksum {checksum} of file {filepath_rel} does't match with checksum {checksum_old}!"
                    )
                    with self._stats_validate_not_match.get_lock():
                        self._stats_validate_not_match.value += 1

                # Check cancel flag
                if self.request_cancel:
                    self._exit(EXIT_CODE_CANCELED)
                    return EXIT_CODE_CANCELED

                # Check pause flag
                if self.request_pause:
                    if not self._pause():
                        self._exit(EXIT_CODE_CANCELED)
                        return EXIT_CODE_CANCELED

                # Increment items counter
                progress_counter += 1

                # Update progress every 100ms
                if time.time() - update_progress_timer_ >= 0.1:
                    self._update_progress_bar_status_bar(progress_counter, size_total_)

            # Update statusbar
            if self._statusbar_show_message_signal is not None:
                self._statusbar_show_message_signal.emit(self.stats_to_statusbar())

            # All stages finished
            logging.info("Meow! ฅ^•ﻌ•^ฅ Validation finished!")

        # Finished with error
        except Exception as e:
            logging.error("Error validating data!", exc_info=e)
            exit_status = EXIT_CODE_ERROR

        # Finished
        self._exit(exit_status)
        return exit_status
