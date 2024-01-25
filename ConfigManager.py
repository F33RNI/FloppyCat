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
import json
import logging
import os

from _version import __version__

# Default config
CONFIG_DEFAULT = {
    "version": __version__,
    "input_paths": [],
    "save_to": "",
    "follow_symlinks": False,
    "delete_data": True,
    "delete_skipped": False,
    "create_empty_dirs": True,
    "generate_tree": True,
    "checksum_alg": "MD5",
    "workload_profile": "Normal",
    "recalculate_checksum": False,
}


class ConfigManager:
    def __init__(self, config_file: str) -> None:
        """Initializes ConfigManager and reads config file

        Args:
            config_file (str): config file (.json)
        """
        self._config_file = config_file

        self._config = {}

        # Try to load config file
        if os.path.exists(config_file):
            logging.info(f"Loading {config_file}")
            with open(config_file, encoding="utf-8") as config_file_stream:
                json_content = json.load(config_file_stream)
                if json_content is not None and isinstance(json_content, dict):
                    self._config = json_content

    def get_config(self, key: str) -> any:
        """Retrieves value from config by key

        Args:
            key (str): config key to get value of

        Returns:
            any | None: key's value from config, CONFIG_DEFAULT or None if not found
        """
        # Retrieve from config
        if key in self._config:
            return self._config[key]

        # Use default value
        elif key in CONFIG_DEFAULT:
            return CONFIG_DEFAULT[key]

        # No key -> show warning and return None
        else:
            logging.warning(f"Key: {key} doesn't exists!")
            return None

    def set_config(self, key: str, value: any) -> None:
        """Updates config values and saves it to the file

        Args:
            key (str): config key
            value (any): key's value
        """
        # Set value
        self._config[key] = value

        # Save to file
        logging.info(f"Saving config to {self._config_file}")
        with open(self._config_file, "w", encoding="utf") as config_file_stream:
            json.dump(self._config, config_file_stream, indent=4)
