#!/bin/bash
# Records hardware and software versions of a campaign (results/<campaign>/versions.txt).
# macOS commands (sysctl, sw_vers); set the same variables as for scripts/run_all.sh.
cd "$(dirname "$0")/.."
S=${SCRATCH:?}; PY=${PYTHON:-./virtualenv/bin/python}
[ -n "${XSB_BIN:-}" ] && export PATH=$XSB_BIN:$PATH
export JAVA_HOME=${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}
echo "date: $(date -u +%FT%TZ)"
echo "machine: $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GB"
echo "os: $(sw_vers -productName) $(sw_vers -productVersion) ($(sw_vers -buildVersion))"
echo "python: $($PY --version 2>&1)"
$PY -c "import duckdb,psycopg2,MySQLdb,pymongo,neo4j,numpy,networkx; print('duckdb', duckdb.__version__); print('psycopg2', psycopg2.__version__); print('mysqlclient', '.'.join(map(str, MySQLdb.version_info[:3]))); print('pymongo', pymongo.__version__); print('neo4j-driver', neo4j.__version__); print('numpy', numpy.__version__); print('networkx', networkx.__version__)"
echo "xsb: $(echo 'halt.' | xsb --version 2>&1 | head -1)"
echo "postgresql: $(psql -d postgres -Atc 'select version()' 2>&1)"
echo "mariadb: $(${MARIADB:-/opt/homebrew/opt/mariadb/bin}/mariadbd --version 2>&1)"
echo "cockroachdb: $(${CRDB:-/opt/homebrew/opt/cockroach/bin/cockroach} version --build-tag 2>&1)"
echo "mongodb: $(mongod --version | head -1)"
echo "neo4j: $(${NEO4J:-/opt/homebrew/opt/neo4j/libexec/bin/neo4j} --version 2>&1 | tail -1), APOC: $(ls $S/neo4j/plugins)"
echo "java: $($JAVA_HOME/bin/java -version 2>&1 | head -1)"
# the SingleStore image is recorded by the singlestore phase of scripts/run_all.sh (Docker runs only then)
echo "trans-bench commit: $(git rev-parse HEAD)$(git diff --quiet HEAD && [ -z "$(git ls-files --others --exclude-standard -- engine systems scripts '*.py')" ] || echo ' + uncommitted changes (code.patch)')"
