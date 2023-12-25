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
import pathlib
import queue
import time

import LoggingHandler

from _control_values import PROCESS_CONTROL_WORK, PROCESS_CONTROL_PAUSE, PROCESS_CONTROL_EXIT

# Timeout waiting for data from directories_to_parse_queue
QUEUE_TIMEOUT = 2

# Definitions for parsed_queue
PATH_IS_FILE = 0
PATH_IS_DIR = 1
PATH_UNKNOWN = 2


def tree_parser(
    directories_to_parse_queue: multiprocessing.Queue,
    parsed_queue: multiprocessing.Queue,
    stats_tree_parsed_dirs: multiprocessing.Value,
    stats_tree_parsed_files: multiprocessing.Value,
    control_value: multiprocessing.Value or None = None,
    logging_queue: multiprocessing.Queue or None = None,
) -> None:
    """Process body to parse directory tree

    NOTE: first root directory will not be added to the parsed_queue!

    Args:
        directories_to_parse_queue (multiprocessing.Queue): queue of directories to parse. Put your initial dir here. Format: (relative parent dir or "", absolute path of root directory, skip)
        parsed_queue (multiprocessing.Queue): parsing results as tuples (relative path, root dir path, PATH_..., is empty directory, skip)
        stats_tree_parsed_dirs (multiprocessing.Value): counter of total successfully parsed directories
        stats_tree_parsed_files (multiprocessing.Value): counter of total successfully parsed files
        control_value (multiprocessing.Value or None, optional): value (int) to pause / cancel process. Defaults to None.
        logging_queue (multiprocessing.Queue or None, optional): logging queue to accept logs. Defaults to None.
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
            parent_dir, root_dir, skip = directories_to_parse_queue.get(block=True, timeout=QUEUE_TIMEOUT)
        except queue.Empty:
            if logging_queue is not None:
                logging.info(f"No more directories to parse! tree_parser() with PID {current_pid} exited")
            return

        # Convert parent dir to absolute path
        parent_dir_abs = os.path.join(root_dir, parent_dir)

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
                # Find path relative to root
                dir_or_file_rel = os.path.relpath(dir_or_file, root_dir)

                # Just file -> put to parsed queue
                if dir_or_file.is_file():
                    # (relative path, root dir path, PATH_..., is empty directory, skip)
                    parsed_queue.put((dir_or_file_rel, root_dir, PATH_IS_FILE, False, skip))

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

                    # (relative path, root dir path, PATH_..., is empty directory, skip)
                    parsed_queue.put((dir_or_file_rel, root_dir, PATH_IS_DIR, is_empty, skip))

                    # Put again in recursion if not empty with the same root
                    if not is_empty:
                        directories_to_parse_queue.put((dir_or_file_rel, root_dir, skip))

                    # Increment counter
                    with stats_tree_parsed_dirs.get_lock():
                        stats_tree_parsed_dirs.value += 1

                # Not a file or directory -> put to parsed queue as PATH_UNKNOWN
                else:
                    # (relative path, root dir path, PATH_..., is empty directory, skip)
                    parsed_queue.put((dir_or_file_rel, root_dir, PATH_UNKNOWN, False, skip))

                    # Increment files counter ¯\_(ツ)_/¯
                    with stats_tree_parsed_files.get_lock():
                        stats_tree_parsed_files.value += 1

            # Error occurred -> log error
            except Exception as e:
                if logging_queue is not None:
                    logging.error(f"Error parsing {str(dir_or_file)} path: {str(e)}")
