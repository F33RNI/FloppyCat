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
import json
import logging
import os

from _version import __version__

# Default config
CONFIG_DEFAULT = {
    "version": __version__,
    "input_paths": [],
    "save_to": "",
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
                if json_content is not None and type(json_content) == dict:
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
