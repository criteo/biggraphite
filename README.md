[![Build Status](https://travis-ci.org/criteo/biggraphite.svg?branch=master)](https://travis-ci.org/criteo/biggraphite)
[![Coverage Status](https://coveralls.io/repos/github/criteo/biggraphite/badge.svg)](https://coveralls.io/github/criteo/biggraphite?branch=master)
[![PyPi version](https://img.shields.io/pypi/v/biggraphite.svg)](https://pypi.python.org/pypi/biggraphite/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/biggraphite.svg)](https://pypi.python.org/pypi/bigraphite/)

BigGraphite
-----------

BigGraphite is a storage layer for timeseries data. It integrates with Graphite as a plugin.

For usage information and how to contribute, please see [CONTRIBUTING.md](CONTRIBUTING.md).

Usage
-----

See [USAGE.md](USAGE.md) and [CONFIGURATION.md](CONFIGURATION.md).

Contact
-------

- [Mailing List](https://groups.google.com/forum/#!forum/biggraphite)


Backends
--------

There is only one supported backend that provides all features: Cassandra, whose design is [described in CASSANDRA_DESIGN.md](CASSANDRA_DESIGN.md).

Another backend supports metadata only, stored in Elasticsearch, [see ELASTICSEARCH_DESIGN.md](ELASTICSEARCH_DESIGN.md).
Using it, it is possible to use Cassandra to store data points and Elasticsearch to store metrics metadata.


Code structure
--------------

- `biggraphite.accessor` exposes the public API to store/retrieve metrics
- `biggraphite.metadata_cache` implements a machine-local cache using [LMDB](https://lmdb.readthedocs.io) so that one does not need a round-trip for each call to `accessor`
- `biggraphite.plugins.*` implements integration with Carbon and Graphite
- `biggraphite.drivers.*` implements the storage backends (eg: Cassandra-specific code)

Disclaimer
----------

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
