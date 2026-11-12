#!/bin/bash
# run.sh <Module> <name> <invariant> KEY=VAL ...
# Writes cfg/<Module>_<name>_<invariant>.cfg, runs TLC on it and logs to out/.
# Needs Java 11+ and the TLA+ tools jar: set TLA2TOOLS=/path/to/tla2tools.jar
# (download from https://github.com/tlaplus/tlaplus/releases).
set -e
cd "$(dirname "$0")"
: "${TLA2TOOLS:?set TLA2TOOLS to the path of tla2tools.jar}"
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
mkdir -p out cfg
mod=$1; name=$2; inv=$3; shift 3
cfg=cfg/${mod}_${name}_${inv}.cfg
out=out/${mod}_${name}_${inv}.txt
{ echo "CONSTANTS"; for kv in "$@"; do echo "  ${kv%%=*} = ${kv#*=}"; done
  echo "SPECIFICATION Spec"; echo "INVARIANT $inv"; } > "$cfg"
meta=$(mktemp -d)
"$JAVA" -Xmx1g -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers 1 -noGenerateSpecTE -deadlock -cleanup \
  -metadir "$meta" -config "$cfg" "$mod.tla" > "$out" 2>&1 || true
rm -rf "$meta"
if grep -q "is violated" "$out"; then r=VIOLATED
elif grep -q "No error has been found" "$out"; then r=HOLDS
else r=ERROR; fi
echo "$mod $name $inv: $r ($(grep -o '[0-9,]* distinct states found' "$out" | head -1))"
