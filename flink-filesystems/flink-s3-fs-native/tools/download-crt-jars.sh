#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Downloads the aws-crt JAR that flink-s3-fs-native needs when
# s3.crt.enabled=true.
#
# Why only aws-crt and not aws-crt-client:
#   aws-crt-client is pure Java and is bundled (shaded) directly into the
#   flink-s3-fs-native fat JAR at build time — no manual placement needed.
#   aws-crt contains JNI-linked native libraries whose C-side FindClass paths
#   are hardcoded, making Maven shade relocation incompatible. It must
#   therefore be placed in the plugin directory with its original class names.
#
# Usage:
#   ./download-crt-jars.sh [OUTPUT_DIR]
#
#   OUTPUT_DIR  Directory to place the JAR in. Defaults to ./crt-jars at the
#               module root. Copy the resulting file to
#               $FLINK_HOME/plugins/s3-fs-native/ alongside
#               flink-s3-fs-native.jar.
#
# Environment:
#   AWS_CRT_VERSION  Override the auto-resolved aws-crt version (rarely
#                    needed; use only when the auto-resolver fails).
#
# Requirements: mvn (Apache Maven) on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
POM_FILE="${MODULE_DIR}/pom.xml"
OUTPUT_DIR="${1:-${MODULE_DIR}/crt-jars}"

if ! command -v mvn >/dev/null 2>&1; then
    echo "ERROR: mvn (Apache Maven) is required but not on PATH." >&2
    exit 1
fi

if [[ ! -f "${POM_FILE}" ]]; then
    echo "ERROR: pom.xml not found at ${POM_FILE}." >&2
    exit 1
fi

# Read the AWS SDK version that the module compiles against.
SDK_VERSION="$(sed -n 's|.*<fs.s3.aws.sdk.version>\([^<]*\)</fs.s3.aws.sdk.version>.*|\1|p' \
    "${POM_FILE}" | head -n1)"
if [[ -z "${SDK_VERSION}" ]]; then
    echo "ERROR: could not read <fs.s3.aws.sdk.version> from ${POM_FILE}." >&2
    exit 1
fi

# Resolve the aws-crt version. aws-crt uses an independent versioning scheme
# from the AWS SDK; reading it from the aws-crt-client dependency tree is the
# authoritative way to stay in sync when the SDK version is bumped.
resolve_crt_version() {
    if [[ -n "${AWS_CRT_VERSION:-}" ]]; then
        echo "${AWS_CRT_VERSION}"
        return
    fi
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    # Clean up the probe directory on any return path from this function.
    trap 'rm -rf "${tmp_dir}"' RETURN
    local tmp_pom="${tmp_dir}/pom.xml"
    cat >"${tmp_pom}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.apache.flink.s3native.tools</groupId>
  <artifactId>crt-version-probe</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <dependencies>
    <dependency>
      <groupId>software.amazon.awssdk</groupId>
      <artifactId>aws-crt-client</artifactId>
      <version>${SDK_VERSION}</version>
    </dependency>
  </dependencies>
</project>
EOF
    mvn -q -f "${tmp_pom}" dependency:list \
        -DincludeGroupIds=software.amazon.awssdk.crt \
        -DincludeArtifactIds=aws-crt \
        -DexcludeTransitive=false \
        -DoutputFile=/dev/stdout 2>/dev/null \
        | awk -F: '/aws-crt/ && $1 ~ /software\.amazon\.awssdk\.crt/ {gsub(/[[:space:]]/, "", $0); print $4; exit}'
}

CRT_VERSION="$(resolve_crt_version || true)"

if [[ -z "${CRT_VERSION}" ]]; then
    echo "ERROR: aws-crt version could not be resolved for aws-crt-client:${SDK_VERSION}." >&2
    echo "       Set AWS_CRT_VERSION explicitly and re-run, e.g." >&2
    echo "       AWS_CRT_VERSION=0.33.6 $0 ${OUTPUT_DIR}" >&2
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"

echo "Downloading aws-crt:${CRT_VERSION} into ${OUTPUT_DIR}"

mvn -q dependency:copy \
    -Dartifact="software.amazon.awssdk.crt:aws-crt:${CRT_VERSION}:jar" \
    -DoutputDirectory="${OUTPUT_DIR}" \
    -Dmdep.stripVersion=false

cat <<EOF

Done. Copy the JAR into your Flink plugin directory:
  cp ${OUTPUT_DIR}/aws-crt-${CRT_VERSION}.jar \$FLINK_HOME/plugins/s3-fs-native/

Then enable CRT in conf/config.yaml:
  s3.crt.enabled: true

Note: aws-crt-client is bundled inside flink-s3-fs-native.jar — no separate
download needed for it.
EOF
