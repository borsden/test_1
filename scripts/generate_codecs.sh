#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCHEMA_PATH=${1:-"$ROOT_DIR/schema/b3-market-data-messages-1.8.0.xml"}
SBE_JAR=${SBE_JAR:-"$ROOT_DIR/third_party/simple-binary-encoding/sbe-all/build/libs/sbe-all-1.38.0-SNAPSHOT.jar"}
OUTPUT_DIR=${OUTPUT_DIR:-"$ROOT_DIR/src/sbe-generated"}

if [[ ! -f "$SCHEMA_PATH" ]]; then
  echo "Schema not found: $SCHEMA_PATH" >&2
  exit 1
fi
if [[ ! -f "$SBE_JAR" ]]; then
  echo "SBE jar not found: $SBE_JAR" >&2
  exit 1
fi

JAVA_OPTS=${JAVA_OPTS:-"-Xmx2g"}
PROPS=(
  -Dsbe.generate.ir=true
  -Dsbe.output.dir="$OUTPUT_DIR"
  -Dsbe.target.language=cpp
  -Dsbe.generate.stubs=true
  -Dsbe.validation.stop.on.error=true
)

set -x
java $JAVA_OPTS "${PROPS[@]}" -jar "$SBE_JAR" "$SCHEMA_PATH"
