"""
 Copyright (C) 2023-2024 Fern Lane, FloppyCat Simple Backup Utility

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


def copy_entries(
    filepaths_queue: multiprocessing.Queue,
    checksums_input: Dict,
    checksums_output: Dict,
    output_dir: str,
    stats_copied_ok_value: multiprocessing.Value,
    stats_copied_error_value: multiprocessing.Value,
    stats_created_dirs_ok_value: multiprocessing.Value,
    stats_created_dirs_error_value: multiprocessing.Value,
    control_value: multiprocessing.Value or None = None,
    logging_queue: multiprocessing.Queue or None = None,
) -> None:
    """Process body to copy input files and directories to the backup output_dir

    Args:
        filepaths_queue (multiprocessing.Queue): queue of non-skipped files to to copy (path relative to root, root dir)
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
        output_dir (str): path to the output (backup) directory
        stats_copied_ok_value (multiprocessing.Value): counter of total successful copy calls
        stats_copied_error_value (multiprocessing.Value): counter of total unsuccessful copy calls
        stats_created_dirs_ok_value (multiprocessing.Value): counter of total successful mkdirs calls
        stats_created_dirs_error_value (multiprocessing.Value): counter of total unsuccessful mkdirs calls
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

            # Raise an error if no input checksum
            if not checksum_input:
                raise Exception(f"No checksum was calculated for {checksum_input}")

            # Generate output absolute path
            output_path_abs = os.path.join(output_dir, filepath_rel)

            # Find output checksum
            checksum_output = None
            if filepath_rel in checksums_output:
                checksum_output = checksums_output[filepath_rel]["checksum"]

            # Skip if file exists and checksums are equal
            if os.path.exists(output_path_abs) and checksum_output and checksum_output == checksum_input:
                continue

            # Try to create directories if not exist
            output_file_base_dir = os.path.dirname(output_path_abs)
            if not os.path.exists(output_file_base_dir):
                try:
                    os.makedirs(output_file_base_dir)
                    with stats_created_dirs_ok_value.get_lock():
                        stats_created_dirs_ok_value.value += 1

                # Ignore dir already exists error
                except FileExistsError:
                    pass

                # Other error occurred -> Log it and increment error counter and skip to next cycle
                except Exception as e:
                    if logging_queue is not None:
                        logging.error(f"Error creating directory {output_file_base_dir}: {str(e)}")
                    with stats_created_dirs_error_value.get_lock():
                        stats_created_dirs_error_value.value += 1
                    continue

            # Copy file
            shutil.copy(input_file_abs, output_path_abs)
            with stats_copied_ok_value.get_lock():
                stats_copied_ok_value.value += 1

        # Error occurred -> log error and increment error counter
        except Exception as e:
            if logging_queue is not None:
                logging.error(f"Error copying {input_file_abs}: {str(e)}")
            with stats_copied_error_value.get_lock():
                stats_copied_error_value.value += 1
