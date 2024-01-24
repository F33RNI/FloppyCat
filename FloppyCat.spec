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
import os
import platform

import PyInstaller.config


PyInstaller.config.CONF["workpath"] = "./build"
# -*- mode: python ; coding: utf-8 -*-


# Final name (without version and platform name)
COMPILE_NAME = "FloppyCat"

# Version of main.py
COMPILE_VERSION = "1.2.0"

# Files and folders to include in final build directory (dist/COMPILE_NAME folder)
INCLUDE_FILES = [
    "gui.ui",
    "stylesheet.qss",
    "README.md",
    "Screenshot.png",
    "icons",
    "fonts",
    "LICENSE",
]

_datas = []
for include_file in INCLUDE_FILES:
    if os.path.isdir(include_file):
        _datas.append((include_file, os.path.basename(include_file)))
    else:
        _datas.append((include_file, "."))
print("datas: {}".format(str(_datas)))

_name = COMPILE_NAME + "-" + COMPILE_VERSION + "-" + str(platform.system() + "-" + str(platform.machine()))

block_cipher = None

a = Analysis(
    ["main.py"],
    pathex=[],
    binaries=[],
    datas=_datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["_bootlocale"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name=_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=["icons/icon.ico"],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name=_name,
)
