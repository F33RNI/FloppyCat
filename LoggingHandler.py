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
import logging.handlers
import multiprocessing
import threading

# Logging level
LOGGING_LEVEL = logging.INFO

# Logging formatter
# FORMATTER_FMT = "[%(asctime)s] [%(filename)17s:%(lineno)4s] [%(levelname)-8s] %(message)s"
FORMATTER_FMT = "[%(asctime)s] [%(levelname)-8s] %(message)s"
FORMATTER_DATEFMT = "%H:%M:%S"


def worker_configurer(queue: multiprocessing.Queue, log_test_message: bool = True):
    """Call this method in your process

    Args:
        queue (multiprocessing.Queue): logging queue
        log_test_message (bool, optional): set to False to disable test log message with process PID. Defaults to True.
    """
    # Remove all current handlers
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Setup queue handler
    queue_handler = logging.handlers.QueueHandler(queue)
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(logging.INFO)

    # Log test message
    if log_test_message:
        logging.info(f"Logging setup is complete for process with PID: {multiprocessing.current_process().pid}")


def _logging_listener(
    logging_queue_: multiprocessing.Queue,
    internal_queue_: multiprocessing.Queue,
    log_formatter_: logging.Formatter,
    console_logging: bool,
) -> None:
    """Main process body to handle logs

    Args:
        logging_queue_ (multiprocessing.Queue): main queue to get log records
        internal_queue_ (multiprocessing.Queue): internal queue to redirect log records
        log_formatter_ (logging.Formatter): formatter
        console_logging (bool): enable console logs
    """
    # Get root logger
    root_logger = logging.getLogger()

    # Remove all current handlers (just in case)
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Setup logging into console
    if console_logging:
        import sys

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter_)
        root_logger.addHandler(console_handler)

    # Set logging level
    root_logger.setLevel(LOGGING_LEVEL)

    # Start queue listener
    while True:
        try:
            # Get logging record
            record = logging_queue_.get()

            # Send None to exit
            if record is None:
                internal_queue_.put(None)
                break

            # Handle current logging record
            logger = logging.getLogger(record.name)
            logger.handle(record)

            # Redirect to internal and external queues
            internal_queue_.put(record)

        # Ignore Ctrl+C (call queue.put(None) to stop this listener)
        except KeyboardInterrupt:
            pass

        # Error! WHY???
        except Exception:
            import sys, traceback

            print("Logging error: ", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


class LoggingHandler:
    def __init__(self):
        # Logging queue
        self.queue = multiprocessing.Queue(-1)

        # Create logs formatter
        self.formatter = logging.Formatter(FORMATTER_FMT, datefmt=FORMATTER_DATEFMT)

        # Internal queue to redirect formatted logs
        self._internal_queue = multiprocessing.Queue(-1)

        # List of external queues to handle log records
        self._external_queues = []

    def _external_queues_listener(self) -> None:
        """Redirects formatted log records from self._internal_queue to self._external_queues
        (will put None to each external queue in case of requested exit)
        """
        logging.info("Starting internal listener to redirect logs")
        while True:
            try:
                # Get one log record
                log_record = self._internal_queue.get(block=True)

                # Check if exit requested
                if log_record is None:
                    # Redirect to external queues
                    for queue in self._external_queues:
                        queue.put(None)
                    break

                # Redirect to external queues
                for queue in self._external_queues:
                    queue.put(log_record)

            # Ignore Ctrl+C (call queue.put(None) to stop this listener)
            except KeyboardInterrupt:
                pass

    def add_external_queue(self, queue: multiprocessing.Queue) -> None:
        """Adds external queue to handle log records

        Args:
            queue (multiprocessing.Queue): queue object to add
        """
        self._external_queues.append(queue)

    def remove_external_queue(self, queue: multiprocessing.Queue) -> None:
        """Removes external queue from handling log records

        Args:
            queue (multiprocessing.Queue): queue object to remove
        """
        self._external_queues.remove(queue)

    def configure_and_start_listener(self, console_logging: bool = True):
        """Initializes logging and starts listening. Send None to queue to stop it

        Args:
            console_logging (bool, optional): True to enable console logging. Defaults to True.
        """

        # Start internal listener as thread
        # Put None to self.queue to stop it
        threading.Thread(target=self._external_queues_listener).start()

        # Start main start listener as process
        # Put None to self.queue to stop it
        logging_handler_process = multiprocessing.Process(
            target=_logging_listener,
            args=(self.queue, self._internal_queue, self.formatter, console_logging),
        )
        logging.info("Starting main logging listener")
        logging_handler_process.start()
        logging.info(f"Main logging listener PID: {logging_handler_process.pid}")
