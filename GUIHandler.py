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

import ctypes
import logging
import multiprocessing
import os
import sys
import threading

from PyQt5 import uic, QtGui, QtCore
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QPushButton,
    QLineEdit,
    QFileDialog,
    QSizePolicy,
    QMessageBox,
    QCheckBox,
)

from _version import __version__
from main import get_resource_path
import ConfigManager
import LoggingHandler
import Backupper


# GUI icon file
ICON_FILE = get_resource_path(os.path.join("icons", "icon.png"))

# GUI file
GUI_FILE = get_resource_path("gui.ui")

# GUI stylesheet
STYLESHEET_FILE = get_resource_path("stylesheet.qss")

# Font file
FONT = get_resource_path(os.path.join("fonts", "W95FA.otf"))


class _Window(QMainWindow):
    time_passed_signal = QtCore.pyqtSignal(str)
    time_left_signal = QtCore.pyqtSignal(str)
    progress_set_value_signal = QtCore.pyqtSignal(int)
    statusbar_show_message_signal = QtCore.pyqtSignal(str)
    logs_append_signal = QtCore.pyqtSignal(str)
    logs_scroll = QtCore.pyqtSignal()
    backup_finished_signal = QtCore.pyqtSignal(int)
    validation_finished_signal = QtCore.pyqtSignal(int)
    backup_paused_resumed_signal = QtCore.pyqtSignal(bool)

    def __init__(
        self,
        config_manager: ConfigManager.ConfigManager,
        logging_handler: LoggingHandler.LoggingHandler,
        backupper: Backupper.Backupper,
    ) -> None:
        """Initializes and opens GUI

        Args:
            config_manager (ConfigManager.ConfigManager): ConfigManager class object
            logging_handler (LoggingHandler.LoggingHandler): LoggingHandler class object to set logs_append_signal
        """
        super(_Window, self).__init__()

        self._config_manager = config_manager
        self._logging_handler = logging_handler
        self._backupper = backupper

        self.backup_active = False
        self.validation_active = False

        self.exit_request = False
        self._close_from_self = False

        # Add internal queue to capture formatted logs
        self._logging_queue = multiprocessing.Queue(-1)
        logging_handler.add_external_queue(self._logging_queue)

        # Load GUI from file
        uic.loadUi(GUI_FILE, self)

        # Set window title
        self.setWindowTitle("FloppyCat Simple Backup Utility " + __version__)

        # Set icon
        self.setWindowIcon(QtGui.QIcon(ICON_FILE))

        # Load font
        if os.path.exists(FONT):
            logging.info(f"Loading font from: {FONT}")
            font_id = QtGui.QFontDatabase.addApplicationFont(FONT)
            font_name = QtGui.QFontDatabase.applicationFontFamilies(font_id)[0]
            self.setFont(QtGui.QFont(font_name, 12 if sys.platform == "darwin" else 10, 300))

        # Show GUI
        self.show()

        # Set stylesheet
        if os.path.exists(STYLESHEET_FILE):
            logging.info(f"Loading {STYLESHEET_FILE} stylesheet")
            with open(STYLESHEET_FILE, "r", encoding="utf-8") as stylesheet_file:
                self.setStyleSheet(stylesheet_file.read())

        # Set QComboBox icons
        combobox_icon_path = get_resource_path(os.path.join("icons", "icons8-dropdown-24.png")).replace("\\", "/")
        self.cob_checksum_alg.setStyleSheet(f"QComboBox::down-arrow {{image: url('{combobox_icon_path}');}}")
        self.cob_workload_profile.setStyleSheet(f"QComboBox::down-arrow {{image: url('{combobox_icon_path}');}}")

        # Set monospace space for logging viewer
        # self.pte_logs.setFont(QFont(QFontDatabase.systemFont(QFontDatabase.FixedFont)))

        # Connect signals
        self.time_passed_signal.connect(self.lb_time_passed.setText)
        self.time_left_signal.connect(self.lb_time_left.setText)
        self.progress_set_value_signal.connect(self.pb_backup.setValue)
        self.statusbar_show_message_signal.connect(self.statusbar.showMessage)
        self.logs_append_signal.connect(self.pte_logs.appendPlainText)
        self.logs_scroll.connect(
            lambda: self.pte_logs.verticalScrollBar().setValue(self.pte_logs.verticalScrollBar().maximum())
        )
        self.backup_finished_signal.connect(lambda status: self.backup_finished_callback(status))
        self.validation_finished_signal.connect(lambda status: self.validation_finished_callback(status))
        self.backup_paused_resumed_signal.connect(lambda paused: self.backup_paused_resumed_callback(paused))

        # Connect buttons
        self.btn_add.clicked.connect(lambda: self.input_add())
        self.btn_save_to_browse.clicked.connect(self.save_to_browse)
        self.btn_start_stop.clicked.connect(self.backup_validation_start_stop)
        self.btn_validate.clicked.connect(self.validation_start)
        self.btn_pause_resume.clicked.connect(self.backup_pause_resume)

        # Set widgets from config
        self.le_save_to.setText(self._config_manager.get_config("save_to"))
        self.cb_delete_data.setChecked(self._config_manager.get_config("delete_data"))
        self.cb_delete_skipped.setChecked(self._config_manager.get_config("delete_skipped"))
        self.cb_delete_skipped.setEnabled(self._config_manager.get_config("delete_data"))
        self.cb_follow_symlinks.setChecked(self._config_manager.get_config("follow_symlinks"))
        self.cb_create_empty_dirs.setChecked(self._config_manager.get_config("create_empty_dirs"))
        self.cb_generate_tree.setChecked(self._config_manager.get_config("generate_tree"))
        self.cob_checksum_alg.setCurrentText(self._config_manager.get_config("checksum_alg"))
        self.cob_workload_profile.setCurrentText(self._config_manager.get_config("workload_profile"))
        self.cb_recalculate_checksum.setChecked(self._config_manager.get_config("recalculate_checksum"))

        # Set input paths from config
        input_paths = self._config_manager.get_config("input_paths")
        for i in range(len(input_paths)):
            self.input_add(i)

        # Connect updaters
        self.le_save_to.textChanged.connect(lambda: self._config_manager.set_config("save_to", self.le_save_to.text()))
        self.cb_follow_symlinks.clicked.connect(
            lambda: self._config_manager.set_config("follow_symlinks", self.cb_follow_symlinks.isChecked())
        )
        self.cb_delete_data.clicked.connect(
            lambda: self._config_manager.set_config("delete_data", self.cb_delete_data.isChecked())
        )
        self.cb_delete_data.clicked.connect(
            lambda: self.cb_delete_skipped.setEnabled(self._config_manager.get_config("delete_data"))
        )
        self.cb_delete_skipped.clicked.connect(
            lambda: self._config_manager.set_config("delete_skipped", self.cb_delete_skipped.isChecked())
        )
        self.cb_create_empty_dirs.clicked.connect(
            lambda: self._config_manager.set_config("create_empty_dirs", self.cb_create_empty_dirs.isChecked())
        )
        self.cb_generate_tree.clicked.connect(
            lambda: self._config_manager.set_config("generate_tree", self.cb_generate_tree.isChecked())
        )
        self.cob_checksum_alg.currentTextChanged.connect(
            lambda: self._config_manager.set_config("checksum_alg", self.cob_checksum_alg.currentText())
        )
        self.cob_workload_profile.currentTextChanged.connect(
            lambda: self._config_manager.set_config("workload_profile", self.cob_workload_profile.currentText())
        )
        self.cb_recalculate_checksum.clicked.connect(
            lambda: self._config_manager.set_config("recalculate_checksum", self.cb_recalculate_checksum.isChecked())
        )

        # Enable close event listening
        self.installEventFilter(self)

        # Start logs redirecting
        logging.info("Starting GUI logs redirecting thread")
        threading.Thread(target=self._logging_redirection_listener).start()

        # Update statusbar
        self.statusbar.showMessage(self._backupper.stats_to_statusbar())

        # Done
        logging.info("GUI loading finished!")

    def backup_validation_start_stop(self) -> None:
        """Starts backup, or request backup / validation cancel"""
        # Start backup
        if not self.backup_active and not self.validation_active:
            # Ask user
            if self.dialog_helper(
                "Start",
                "Are you sure you want to start the backup?\nPlease do not modify files during backup!",
                ask=True,
            ):
                # Check flags again (just in case)
                if not self.backup_active and not self.validation_active:
                    # Check if we have any entries
                    if len(self._config_manager.get_config("input_paths")) == 0:
                        logging.error("Error starting backup! No entries to backup!")
                        self.dialog_helper(
                            "Error starting backup",
                            "No files to backup!",
                            "Please add entries by pressing '+' button",
                            QMessageBox.Critical,
                        )
                        return

                    # Try to parse them
                    try:
                        input_entries = self._backupper.parse_input_entries()
                    except Exception as e:
                        logging.error("Error parsing input entries", exc_info=e)
                        self.dialog_helper(
                            "Error starting backup",
                            "Error parsing input entries",
                            str(e),
                            QMessageBox.Critical,
                        )
                        return
                    if len(input_entries) == 0:
                        logging.error("Error starting backup! No valid entries to backup!")
                        self.dialog_helper(
                            "Error starting backup",
                            "No valid entries to backup!",
                            "Please specify at least one existing file / directory",
                            QMessageBox.Critical,
                        )
                        return

                    # Check output directory format
                    output_dir = self._config_manager.get_config("save_to").strip()
                    try:
                        output_dir = os.path.normpath(output_dir)
                    except:
                        output_dir = None
                    if not output_dir:
                        logging.error("Error starting backup! No or wrong output path!")
                        self.dialog_helper(
                            "Error starting backup",
                            "No or wrong output path!",
                            "Please specify output directory where to save backup",
                            QMessageBox.Critical,
                        )
                        return

                    # Disable GUI elements
                    self.gui_set_enabled(False)

                    # Change buttons
                    self.btn_start_stop.setText("Cancel")
                    self.btn_pause_resume.setText("Pause")
                    self.btn_pause_resume.setEnabled(True)

                    # Start backup as thread
                    logging.info("Starting backup as thread")
                    threading.Thread(
                        target=self._backupper.start_backup,
                        args=(
                            input_entries,
                            output_dir,
                            self.time_passed_signal,
                            self.time_left_signal,
                            self.progress_set_value_signal,
                            self.statusbar_show_message_signal,
                            self.backup_paused_resumed_signal,
                            self.backup_finished_signal,
                        ),
                    ).start()
                    self.backup_active = True

        # Stop backup / validation (cancel)
        else:
            # Ask user
            if self.dialog_helper(
                "Cancel",
                f"Are you sure you want to stop the {'backup' if self.backup_active else 'validation'}?",
                ask=True,
            ):
                # If still running
                if self.backup_active or self.validation_active:
                    # Send request
                    self._backupper.request_cancel = True

                    # Change buttons
                    self.btn_start_stop.setEnabled(False)
                    self.btn_pause_resume.setEnabled(False)

    def validation_start(self) -> None:
        """Starts validation"""
        # Start validation
        if not self.backup_active and not self.validation_active:
            # Ask user
            if self.dialog_helper(
                "Start",
                "Are you sure you want to start backup validation?",
                ask=True,
            ):
                # Check flags again (just in case)
                if not self.backup_active and not self.validation_active:
                    # Check output directory format
                    output_dir = self._config_manager.get_config("save_to").strip()
                    try:
                        output_dir = os.path.normpath(output_dir)
                    except:
                        output_dir = None
                    if not output_dir:
                        logging.error("Error starting validation! No or wrong output path!")
                        self.dialog_helper(
                            "Error starting validation",
                            "No or wrong output path!",
                            "Please specify backup directory to validate",
                            QMessageBox.Critical,
                        )
                        return

                    # Check if output directory is dir and exists
                    if not os.path.exists(output_dir) or not os.path.isdir(output_dir):
                        logging.error("Error starting validation! No or wrong output path!")
                        self.dialog_helper(
                            "Error starting validation",
                            "Output directory path is wrong or doesn't exist!",
                            "Please specify correct and not empty backup directory to validate",
                            QMessageBox.Critical,
                        )
                        return

                    # Disable GUI elements
                    self.gui_set_enabled(False)

                    # Change buttons
                    self.btn_start_stop.setText("Cancel")
                    self.btn_pause_resume.setEnabled(False)

                    # Start validation as thread
                    logging.info("Starting validation as thread")
                    threading.Thread(
                        target=self._backupper.validate,
                        args=(
                            output_dir,
                            self.time_passed_signal,
                            self.time_left_signal,
                            self.progress_set_value_signal,
                            self.statusbar_show_message_signal,
                            self.validation_finished_signal,
                        ),
                    ).start()
                    self.validation_active = True

    def backup_pause_resume(self) -> None:
        """Requests backup pause / resume"""
        # Ignore if any flag is set
        if self._backupper.request_pause or self._backupper.request_resume or self._backupper.request_cancel:
            return

        # Resume
        if self._backupper.paused:
            self._backupper.request_resume = True
            self.btn_pause_resume.setEnabled(False)

        # Pause
        else:
            # Ask user
            if self.dialog_helper("Pause", "Are you sure you want to pause the backup?", ask=True):
                # Ignore if any flag is set
                if self._backupper.request_pause or self._backupper.request_resume or self._backupper.request_cancel:
                    return

                # Check current state again
                if not self._backupper.paused:
                    self._backupper.request_pause = True
                    self.btn_pause_resume.setEnabled(False)

    def backup_finished_callback(self, status: int) -> None:
        """Callback on backup finish

        Args:
            status (int): EXIT_CODE_SUCCESSFULLY / EXIT_CODE_ERROR / EXIT_CODE_CANCELED
        """
        # Exit if requested
        if self.exit_request:
            self._close_from_self = True
            self.close()
            return

        # Clear flag
        self.backup_active = False

        # Enable GUI back
        self.gui_set_enabled(True)

        # Restore buttons
        self.btn_start_stop.setText("Start")
        self.btn_start_stop.setEnabled(True)
        self.btn_pause_resume.setText("Pause")
        self.btn_pause_resume.setEnabled(False)

        # Log and show message box
        if status == Backupper.EXIT_CODE_SUCCESSFULLY:
            logging.info("Backup finished without error")
            self.dialog_helper("Finished", "Meow! ฅ^•ﻌ•^ฅ Backup finished!", self._backupper.stats_to_str())
        elif status == Backupper.EXIT_CODE_CANCELED:
            logging.info("(╯°□°）╯︵ ┻━┻ Backup cancelled!")
            self.dialog_helper(
                "Finished", "(╯°□°）╯︵ ┻━┻ Backup cancelled!", self._backupper.stats_to_str(), QMessageBox.Warning
            )
        else:
            logging.error("¯(҂◡_◡)  Backup finished with error!")
            self.dialog_helper(
                "Finished",
                "¯(҂◡_◡) Backup finished with error!",
                "Please see logs for more info",
                QMessageBox.Critical,
            )

        # Reset process bar
        self.pb_backup.setValue(0)

    def backup_paused_resumed_callback(self, paused: bool) -> None:
        """Callback on backup paused / resumed

        Args:
            paused (bool): True if paused, False if resumed
        """
        # Change button
        self.btn_pause_resume.setText("Resume" if paused else "Pause")
        self.btn_pause_resume.setEnabled(True)

        # Show message box
        self.dialog_helper(
            "Paused" if paused else "Resumed", f"{'◔_◔ Backup paused' if paused else 'Backup (•̀ᴗ•́)و ̑̑ resumed'}!"
        )

    def validation_finished_callback(self, status: int) -> None:
        """Callback on validation finish

        Args:
            status (int): EXIT_CODE_SUCCESSFULLY / EXIT_CODE_ERROR / EXIT_CODE_CANCELED
        """
        # Exit if requested
        if self.exit_request:
            self._close_from_self = True
            self.close()
            return

        # Clear flag
        self.validation_active = False

        # Enable GUI back
        self.gui_set_enabled(True)

        # Restore buttons
        self.btn_start_stop.setText("Start")
        self.btn_start_stop.setEnabled(True)
        self.btn_pause_resume.setText("Pause")
        self.btn_pause_resume.setEnabled(False)

        # Log and show message box
        if status == Backupper.EXIT_CODE_SUCCESSFULLY:
            logging.info("Validation finished without error")
            self.dialog_helper(
                "Finished", "Meow! ฅ^•ﻌ•^ฅ Validation finished!", self._backupper.validation_stats_to_str()
            )
        elif status == Backupper.EXIT_CODE_CANCELED:
            logging.info("(╯°□°）╯︵ ┻━┻ Validation cancelled!")
            self.dialog_helper("Finished", "(╯°□°）╯︵ ┻━┻ Validation cancelled!", icon=QMessageBox.Warning)
        else:
            logging.error("(҂◡_◡) Validation finished with error!")
            self.dialog_helper(
                "Finished",
                "(҂◡_◡) Validation finished with error!",
                "Please see logs for more info",
                QMessageBox.Critical,
            )

        # Reset process bar
        self.pb_backup.setValue(0)

    def gui_set_enabled(self, enabled: bool = True) -> None:
        """Enables or disables GUI widgets

        Args:
            enabled (bool, optional): True to enable False to disable. Defaults to True.
        """
        logging.info(f"{'Enabling' if enabled else 'Disabling'} GUI widgets")
        self.groupBox_3.setEnabled(enabled)
        self.le_save_to.setEnabled(enabled)
        self.btn_save_to_browse.setEnabled(enabled)
        self.cb_follow_symlinks.setEnabled(enabled)
        self.cb_delete_data.setEnabled(enabled)
        self.cb_delete_skipped.setEnabled(self._config_manager.get_config("delete_data") if enabled else False)
        self.cb_create_empty_dirs.setEnabled(enabled)
        self.cb_generate_tree.setEnabled(enabled)
        self.cob_checksum_alg.setEnabled(enabled)
        self.cob_workload_profile.setEnabled(enabled)
        self.cb_recalculate_checksum.setEnabled(enabled)
        self.btn_validate.setEnabled(enabled)

    def input_add(self, index: int = -1, insert_index: int = -1) -> None:
        """Adds new input fields

        Args:
            index (int, optional): _description_. Defaults to -1.
            insert_index (int, optional): _description_. Defaults to -1.
        """
        path = ""
        skip = False
        input_paths = self._config_manager.get_config("input_paths")
        if index >= 0:
            path = input_paths[index]["path"]
            skip = input_paths[index]["skip"]
        else:
            input_paths.append({"path": path, "skip": skip})
            self._config_manager.set_config("input_paths", input_paths)

        logging.info(f"Adding new input path {path}")

        # Create widgets
        layout = QHBoxLayout()
        le_path = QLineEdit(path)
        le_path.setToolTip("File / directory path to backup")

        btn_browse_file = QPushButton()
        btn_browse_file.setIcon(QIcon(get_resource_path(os.path.join("icons", "icons8-add-file-24.png"))))
        btn_browse_file.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        btn_browse_file.setToolTip("Browse file")
        btn_browse_dir = QPushButton()
        btn_browse_dir.setIcon(QIcon(get_resource_path(os.path.join("icons", "icons8-add-folder-24.png"))))
        btn_browse_dir.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        btn_browse_dir.setToolTip("Browse directory")
        btn_move_up = QPushButton()
        btn_move_up.setIcon(QIcon(get_resource_path(os.path.join("icons", "icons8-up-24.png"))))
        btn_move_up.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        btn_move_up.setToolTip("Move up (increase priority)")
        btn_move_down = QPushButton()
        btn_move_down.setIcon(QIcon(get_resource_path(os.path.join("icons", "icons8-down-24.png"))))
        btn_move_down.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        btn_move_down.setToolTip("Move down (decrease priority)")

        cb_skip = QCheckBox("Skip")
        cb_skip.setChecked(skip)
        cb_skip.setToolTip("Skip file / directory from current backup")

        btn_remove = QPushButton()
        btn_remove.setIcon(QIcon(get_resource_path(os.path.join("icons", "icons8-close-24.png"))))
        btn_remove.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        btn_remove.setToolTip("Remove entry")

        # Connect edit events and buttons
        le_path.textChanged.connect(lambda: self.input_edit(layout, path=le_path.text()))
        cb_skip.clicked.connect(lambda: self.input_edit(layout, skip=cb_skip.isChecked()))
        btn_browse_file.clicked.connect(lambda: self.input_browse(layout, browse_file=True))
        btn_browse_dir.clicked.connect(lambda: self.input_browse(layout, browse_file=False))
        btn_move_up.clicked.connect(lambda: self.input_move(layout, move_up=True))
        btn_move_down.clicked.connect(lambda: self.input_move(layout, move_up=False))
        btn_remove.clicked.connect(lambda: self.input_remove(layout))

        # Add widgets to the new layout
        layout.addWidget(le_path)
        layout.addWidget(btn_browse_file)
        layout.addWidget(btn_browse_dir)
        layout.addWidget(btn_move_up)
        layout.addWidget(btn_move_down)
        layout.addWidget(cb_skip)
        layout.addWidget(btn_remove)
        layout.setContentsMargins(0, 0, 0, 0)

        # Add to V box layout
        self.vl_inputs.insertLayout(insert_index, layout)

    def input_edit(self, h_box_layout: QHBoxLayout, path: str or None = None, skip: bool or None = None) -> None:
        """Edits input path / skip

        Args:
            h_box_layout (QHBoxLayout): _description_
            path (str or None, optional): _description_. Defaults to None.
            skip (bool or None, optional): _description_. Defaults to None.
        """
        if h_box_layout is not None:
            index = self.vl_inputs.indexOf(h_box_layout)
            logging.info(f"Editing input path with index: {index}")

            input_paths = self._config_manager.get_config("input_paths")
            if path is not None:
                input_paths[index]["path"] = path.strip()
            if skip is not None:
                input_paths[index]["skip"] = skip
            self._config_manager.set_config("input_paths", input_paths)

    def input_browse(self, h_box_layout: QHBoxLayout, browse_file: bool = True):
        """Opens file browser

        Args:
            h_box_layout (QHBoxLayout): _description_
            browse_file (bool, optional): _description_. Defaults to True.
        """
        if h_box_layout is not None:
            index = self.vl_inputs.indexOf(h_box_layout)
            logging.info(f"Browsing file / dir for input path with index: {index}")

            # Get input path from config
            input_paths = self._config_manager.get_config("input_paths")

            # File
            if browse_file:
                options = QFileDialog.Options()
                file_dialog = QFileDialog(self)
                file_name, _ = file_dialog.getOpenFileName(
                    self,
                    "Select single image file",
                    input_paths[index]["path"],
                    "All Files (*)",
                    options=options,
                )
                if file_name:
                    input_paths[index]["path"] = file_name
                    h_box_layout.itemAt(0).widget().setText(file_name)
                    self._config_manager.set_config("input_paths", input_paths)

            # Dir
            else:
                options = QFileDialog.Options()
                folder_dialog = QFileDialog.getExistingDirectory(
                    self,
                    "Select folder containing images",
                    input_paths[index]["path"],
                    options=options,
                )
                if folder_dialog:
                    input_paths[index]["path"] = folder_dialog
                    h_box_layout.itemAt(0).widget().setText(folder_dialog)
                    self._config_manager.set_config("input_paths", input_paths)

    def input_move(self, h_box_layout: QHBoxLayout, move_up: bool = True) -> None:
        """Moves entry up or down

        Args:
            h_box_layout (QHBoxLayout): _description_
            move_up (bool, optional): _description_. Defaults to True.
        """
        index = self.vl_inputs.indexOf(h_box_layout)
        input_paths = list(self._config_manager.get_config("input_paths"))

        # Ignore out of bounds
        if (index == 0 and move_up) or index == len(input_paths) - 1 and not move_up:
            return

        logging.info(f"Moving {'up' if move_up else 'down'} input path with index: {index}")

        # Save current data and remove it
        current_data = input_paths[index]
        self.input_remove(h_box_layout)

        # Retrieve new data
        input_paths = list(self._config_manager.get_config("input_paths"))

        # Insert
        index_new = index - 1 if move_up else index + 1
        input_paths.insert(index_new, current_data)
        self._config_manager.set_config("input_paths", input_paths)

        # Add input entry
        self.input_add(index_new, index_new)

    def input_remove(self, h_box_layout: QHBoxLayout) -> None:
        """Removes input widgets

        Args:
            h_box_layout (QHBoxLayout): _description_
        """
        index = self.vl_inputs.indexOf(h_box_layout)
        logging.info(f"Removing input path with index: {index}")

        # Remove all widgets and H box layout from V box layout
        while h_box_layout.count() > 0:
            widget = h_box_layout.itemAt(0).widget()
            h_box_layout.removeWidget(widget)
            widget.deleteLater()
        self.vl_inputs.removeItem(h_box_layout)
        h_box_layout.deleteLater()

        # Remove from paths
        input_paths = self._config_manager.get_config("input_paths")
        del input_paths[index]
        self._config_manager.set_config("input_paths", input_paths)

    def save_to_browse(self) -> None:
        """Asks users to select output directory"""
        options = QFileDialog.Options()
        folder_dialog = QFileDialog.getExistingDirectory(
            self, "Select directory where to save backup", self._config_manager.get_config("save_to"), options=options
        )
        if folder_dialog:
            self._config_manager.set_config("save_to", folder_dialog)
            self.le_save_to.setText(folder_dialog)

    def dialog_helper(
        self,
        title: str,
        message: str,
        additional_text: str = "",
        icon: QMessageBox.Icon = QMessageBox.Information,
        ask: bool = False,
    ) -> bool or None:
        """Shows dialog or message box

        Args:
            title (str): dialog / message box title
            message (str): text / message
            additional_text (str): additional text (for message box only)
            icon (QMessageBox.Icon): message box icon (for message box only)
            ask (bool, optional): True for dialog, False for message box. Defaults to False.

        Returns:
            bool or None: dialog result or None in case of message box
        """
        # Dialog
        if ask:
            reply = QMessageBox.question(self, title, message, QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            return reply == QMessageBox.Yes

        # Regular message box
        else:
            message_box = QMessageBox(self)
            message_box.setIcon(icon)
            message_box.setWindowTitle(title)
            message_box.setText(message)
            if additional_text:
                message_box.setInformativeText(additional_text)
            message_box.exec_()

    def _logging_redirection_listener(self) -> None:
        """Background thread body that redirects logs to the QPlainTextEdit"""
        while True:
            # Get one log record
            log_record = self._logging_queue.get(block=True)

            # Check if exit requested
            if log_record is None:
                break

            # Format it
            log_formatted = self._logging_handler.formatter.format(log_record)

            # Send to QPlainTextEdit and scroll it to the end and exit from loop in case of error
            try:
                self.logs_append_signal.emit(log_formatted)
                self.logs_scroll.emit()
            except Exception:
                break

    def closeEvent(self, event) -> None:
        """Closes app (asks user before it if we have opened file)

        Args:
            event (_type_): _description_
        """
        if not self._close_from_self and (self.backup_active or self.validation_active):
            if self.dialog_helper("Quit", "Are you sure you want to stop and quit?", ask=True):
                # Stop backup / validation
                if self.backup_active or self.validation_active:
                    # Send request
                    self._backupper.request_cancel = True

                    # Change buttons
                    self.btn_start_stop.setEnabled(False)
                    self.btn_pause_resume.setEnabled(False)

                    # Request exit
                    self.exit_request = True

            # Clear flag (just in case)
            self._close_from_self = False

            # Ignore anyway
            event.ignore()

        # No file -> Exit without asking
        else:
            logging.info("Closing GUI")

            # Stop logging handler
            self._logging_queue.put(None)
            self._logging_handler.remove_external_queue(self._logging_queue)

            # Accept event
            event.accept()


class GUIHandler:
    def __init__(self, config_manager: ConfigManager.ConfigManager):
        self._config_manager = config_manager

    def start_gui(self, logging_handler: LoggingHandler.LoggingHandler, backupper: Backupper.Backupper) -> int:
        """Starts GUI (blocking)

        Args:
            logging_handler (LoggingHandler.LoggingHandler): LoggingHandler class object
            backupper (Backupper.Backupper): Backupper class object

        Returns:
            int: QApplication Window exit code
        """
        # Replace icon in taskbar
        if os.name == "nt":
            logging.info("Replacing icon in taskbar")
            app_ip = "f3rni.floppycat.floppycat." + __version__
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(app_ip)

        # Start app
        logging.info("Opening GUI")
        app = QApplication.instance() or QApplication(sys.argv)
        app.setStyle("windows")
        _ = _Window(self._config_manager, logging_handler, backupper)
        exit_code = app.exec_()
        logging.info("GUI closed")
        return exit_code
