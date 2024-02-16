![MIT Licence](https://badgen.net/static/license/MIT/blue) ![Python >=3.10](https://badgen.net/badge/python/3.10/blue)

# 🏹RobinHood
RobinHood (shortened RH) is a file synchronisation application that provides a [FreeFileSync](https://freefilesync.org/)-inspired front-end to [``rclone``](https://rclone.org/).

**This application is still a work-in-progress** and is currently not available through PIP. You are welcome to download and use it (please read carefully licence and disclaimer).

## 📥Installation
Python >=3.10 is required to run this application. Furthermore, the following packages are required:
- textual==0.38.1
- psutil==5.9.5
- platformdirs==3.11.0
- aiofiles==23.2.1
- bigtree[pandas]==0.14.3

They can be easily installed by running

    $ pip install -r requirements.txt

I also made a specific library to interact with ``rclone``. As this is not currently available with PIP, it needs to be downloaded. By cloning this repository, it should also add ``pyrclone`` library from the other repository. However, if anything goes bad, please also clone the following repository: https://github.com/valerio-afk/pyrclone

## 🏃Quick Start

    $ python rh.py

## 👥Profiles
It is highly recommended the creation of profiles to keep track of source and destination paths, file/path filters, and much more. This can be done via command line (``python rh.py profile --help``) or via the user interface (``CTRL+P`` allows you to save the current configurations).

To open RH with your profile, simply write

    $ python rh.py <method> -p <your_profile>
where method can be one of the following:
- ``update:`` to transfer new files from source to destination
- ``mirror:`` to make destination *exactly* as the source
- ``synch:`` bi-directional synching
- ``dedupe:`` to identify deduplicates

These actions will not be performed and you can change it once the user interface appears.

## ⚠️Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.