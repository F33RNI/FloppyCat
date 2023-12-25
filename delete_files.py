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
import logging
import multiprocessing
import os
import queue
import shutil
import time
from typing import Dict

import LoggingHandler

from _control_values import PROCESS_CONTROL_WORK, PROCESS_CONTROL_PAUSE, PROCESS_CONTROL_EXIT

# Timeout waiting for data from output_tree_queue
QUEUE_TIMEOUT = 5


def delete_files(
    output_tree_queue: multiprocessing.Queue,
    input_tree: Dict,
    delete_skipped: bool,
    stats_deleted_ok_value: multiprocessing.Value,
    stats_deleted_error_value: multiprocessing.Value,
    control_value: multiprocessing.Value or None = None,
    logging_queue: multiprocessing.Queue or None = None,
) -> None:
    """Process body to delete files from existing backup according to input tree

    Args:
        output_files_tree_queue (multiprocessing.Queue): output tree as Queue (tree_type, filepath_rel, root_skip_empty)
        input_tree (Dict): tree of all input files and directories
        delete_skipped (bool): True to also delete skipped files
        stats_deleted_ok_value (multiprocessing.Value): counter of total successful delete calls
        stats_deleted_error_value (multiprocessing.Value): counter of total unsuccessful delete calls
        control_value (multiprocessing.Value or None, optional): value (int) to pause / cancel process. Defaults to None.
        logging_queue (multiprocessing.Queue or None, optional): logging queue to accept logs. Defaults to None.
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
            tree_type, filepath_rel, root_skip_empty = output_tree_queue.get(block=True, timeout=QUEUE_TIMEOUT)
        except queue.Empty:
            if logging_queue is not None:
                logging.info(f"No more files check and delete! delete_files() with PID {current_pid} exited")
            return

        try:
            # Parse data
            root = root_skip_empty["root"]
            skip = root_skip_empty["skip"]
            empty = False
            if "empty" in root_skip_empty:
                empty = root_skip_empty["empty"]

            # NOTE: if everything is ok, root here must be always the same an it must be backup directory

            # Skip (if everything is ok, this should never happen)
            if skip:
                continue

            # Convert to absolute path
            out_filepath_abs = os.path.join(root, filepath_rel)

            # Skip if not exists
            if not os.path.exists(out_filepath_abs):
                continue

            # Try to find this path inside input_tree and find out if need to delete it
            delete_flag = True
            if filepath_rel in input_tree[tree_type]:
                skip_ = input_tree[tree_type][filepath_rel]["skip"]
                if not (delete_skipped and skip_):
                    delete_flag = False

            # Skip if we don't need to delete it
            if not delete_flag:
                continue

            # Must be 1st
            if tree_type == "files":
                # Delete as file
                os.remove(out_filepath_abs)
                with stats_deleted_ok_value.get_lock():
                    stats_deleted_ok_value.value += 1

            # Must be 2nd
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

            # "unknown" Must be 3rd
            # Idk what exactly we should do here, so first we delete it as a file and then as a directory
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
