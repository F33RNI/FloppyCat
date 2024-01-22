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
import hashlib
import logging
import multiprocessing
import os
import queue
import string
import time
from typing import Dict

import LoggingHandler

from _control_values import PROCESS_CONTROL_WORK, PROCESS_CONTROL_PAUSE, PROCESS_CONTROL_EXIT

# Block size (in bytes) for calculating checksum
BLOCK_SIZE = 4096

# Timeout waiting for data from filepaths_queue
QUEUE_TIMEOUT = 2


def parse_checksums_from_file(checksum_file: str, root_dir: str, checksum_alg: str) -> Dict:
    """Extracts checksums from file as dictionary

    Args:
        checksum_file (str): path to file that stores checksums
        NOTE: Paths in the checksum_file must be relative to root_dir
        root_dir (str): root directory (just to write to the "root" key in output)
        checksum_alg (str): MD5 / SHA256 / SHA512 for checksum length check

    Returns:
        Dict:
        {
            "file_1_relative_to_root_path": {
                "root": root_dir,
                "checksum": "file_1_checksum"
            },
            "file_2_relative_to_root_path": {
                "root": root_dir,
                "checksum": "file_2_checksum"
            }
        }
    """
    # Skip if no checksums file
    if not os.path.exists(checksum_file):
        return {}

    try:
        logging.info(f"Parsing checksums from {checksum_file}")
        checksums = {}
        with open(checksum_file, "r", encoding="utf8") as fileobject:
            for line in fileobject:
                # Check line
                if not line:
                    continue

                # Remove any new line symbols (just in case) and starting and leading spaces
                line = line.replace("\n", "").replace("\r", "").strip()

                # Skip empty
                if not line:
                    continue

                # Check line start to be hexadecimal
                if line[0] not in string.hexdigits:
                    continue

                # Split by * (checksums file have a structure: checksum *filepath)
                line_splitted = line.split("*")

                # Check it
                if len(line_splitted) <= 1:
                    continue

                # Extract checksum
                checksum = line_splitted[0].strip()

                # Check length
                if (
                    (checksum_alg.lower() == "md5" and len(checksum) != 32)
                    or (checksum_alg.lower() == "sha256" and len(checksum) != 64)
                    or (checksum_alg.lower() == "sha512" and len(checksum) != 128)
                ):
                    continue

                # Check it to be hexadecimal
                for character in checksum:
                    if character not in string.hexdigits:
                        continue

                # Extract path
                filepath = "*".join(line_splitted[1:])

                # Check it
                if not filepath:
                    continue

                # Try to normalize
                try:
                    filepath = os.path.normpath(filepath)
                except Exception:
                    continue

                # Finally, append to dict
                checksums[filepath] = {"root": root_dir, "checksum": checksum}

        # Print stats
        logging.info(f"Found {len(checksums)} checksums")

        # Return them
        return checksums
    except Exception as e:
        logging.error(f"Error parsing checksums for file {checksum_file}!", exc_info=e)
    return {}


def calculate_checksums(
    checksum_out_file: str or None,
    checksum_out_queue: multiprocessing.Queue or None,
    checksum_alg: str,
    filepaths_queue: multiprocessing.Queue,
    output_as_absolute_paths: bool,
    stats_checksums_calculate_ok_value: multiprocessing.Value,
    stats_checksums_calculate_error_value: multiprocessing.Value,
    control_value: multiprocessing.Value or None = None,
    checksum_out_file_lock: multiprocessing.Lock or None = None,
    logging_queue: multiprocessing.Queue or None = None,
) -> None:
    """Process body to calculate checksum of each file in filepaths_queue

    Args:
        checksum_out_file (str or None): file to append (a+) checksums or None to don't write to it
        checksum_out_queue (multiprocessing.Queue or None): queue to handle calculated checksums
        (path relative to root, root dir, checksum) or None
        checksum_alg (str): MD5 / SHA256 / SHA512
        filepaths_queue (multiprocessing.Queue): queue of non-skipped files to calculate checksums
        (path relative to root, root dir)
        output_as_absolute_paths (bool): True to write filepaths as absolute paths instead of relative to root dir,
        only matters if checksum_out_file is specified
        stats_checksums_calculate_ok_value (multiprocessing.Value): counter of total successful checksum calculations
        stats_checksums_calculate_error_value (multiprocessing.Value): counter of total unsuccessful
        checksum calculations
        control_value (multiprocessing.Value or None, optional): value (int) to pause / cancel process.
        checksum_out_file_lock (multiprocessing.Lock or None, optional). Lock to prevent writing to one file
         as the same time.
        logging_queue (multiprocessing.Queue or None, optional): logging queue to accept logs.
    """
    # Setup logging
    if logging_queue is not None:
        LoggingHandler.worker_configurer(logging_queue, False)

    # Log current PID
    current_pid = multiprocessing.current_process().pid
    if logging_queue is not None:
        logging.info(f"calculate_checksums() with PID {current_pid} started")

    while True:
        # Check control
        if control_value is not None:
            # Get current control value
            with control_value.get_lock():
                control_value_ = control_value.value

            # Pause
            if control_value_ == PROCESS_CONTROL_PAUSE:
                if logging_queue is not None:
                    logging.info(f"calculate_checksums() with PID {current_pid} paused")

                # Sleep loop
                while True:
                    # Retrieve updated value
                    with control_value.get_lock():
                        control_value_ = control_value.value

                    # Resume?
                    if control_value_ == PROCESS_CONTROL_WORK:
                        if logging_queue is not None:
                            logging.info(f"calculate_checksums() with PID {current_pid} resumed")
                        break

                    # Exit?
                    elif control_value_ == PROCESS_CONTROL_EXIT:
                        if logging_queue is not None:
                            logging.info(f"calculate_checksums() with PID {current_pid} exited upon request")
                        return

                    # Sleep some time
                    time.sleep(0.1)

            # Exit
            elif control_value_ == PROCESS_CONTROL_EXIT:
                # Check output queue
                if checksum_out_queue is None or checksum_out_queue.empty():
                    if logging_queue is not None:
                        logging.info(f"calculate_checksums() with PID {current_pid} exited upon request")
                    return
                # Output is not empty -> just wait
                else:
                    time.sleep(0.1)

        # Retrieve from queue and exit process on timeout
        try:
            filepath_rel, filepath_root_dir = filepaths_queue.get(block=True, timeout=QUEUE_TIMEOUT)
        except queue.Empty:
            if logging_queue is not None:
                logging.info(
                    f"No more files to calculate checksum for! calculate_checksums() with PID {current_pid} exited"
                )
            return

        # Convert to absolute path
        filepath_abs = os.path.join(filepath_root_dir, filepath_rel)

        try:
            # Skip or exit if no file to calculate checksum of
            if not os.path.exists(filepath_abs):
                continue

            # Calculate checksum in blocks
            local_hash = hashlib.new(checksum_alg.lower())
            with open(filepath_abs, "rb") as f:
                for byte_block in iter(lambda: f.read(BLOCK_SIZE), b""):
                    local_hash.update(byte_block)

            # Calculate final checksum
            checksum = local_hash.hexdigest()

            # Determine how we need to write filepath
            filepath_write = filepath_abs if output_as_absolute_paths else filepath_rel

            # Write to file with lock
            if checksum_out_file is not None:
                if checksum_out_file_lock is not None:
                    checksum_out_file_lock.acquire()
                with open(checksum_out_file, "a+", encoding="utf-8") as checksum_file_stream:
                    checksum_file_stream.write(f"{checksum} *{filepath_write}\n")
                if checksum_out_file_lock is not None:
                    checksum_out_file_lock.release()

            # Write to the queue
            if checksum_out_queue is not None:
                checksum_out_queue.put((filepath_rel, filepath_root_dir, checksum))

            # Increment stats
            with stats_checksums_calculate_ok_value.get_lock():
                stats_checksums_calculate_ok_value.value += 1

        # Error occurred -> log error and increment error counter
        except Exception as e:
            if logging_queue is not None:
                logging.error(f"Error calculating checksum for file {filepath_abs}: {str(e)}")
            with stats_checksums_calculate_ok_value.get_lock():
                stats_checksums_calculate_error_value.value += 1
