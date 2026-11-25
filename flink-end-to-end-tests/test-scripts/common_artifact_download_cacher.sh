#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This bash script aims to predownload dependency tarballs for the E2E tests.

if [ -z "${E2E_TARBALL_CACHE:-}" ] ; then
    echo "You have to export the E2E Tarball Cache as E2E_TARBALL_CACHE"
    exit 1
fi

mkdir -p $E2E_TARBALL_CACHE

# Given a URL and an optional expected SHA-512 hex digest, checks whether a valid file exists in
# E2E_TARBALL_CACHE, otherwise downloads it atomically (failing on HTTP errors) and verifies the
# checksum before echoing the path to the cached file.
# For example:
# result=$(get_artifact https://archive.apache.org/artifact.tar.gz <optional-sha512>)
# echo $result

function verify_sha512 {
    local file_path=$1
    local expected_sha512=$2
    local actual_sha512
    if command -v sha512sum >/dev/null 2>&1; then
        actual_sha512=$(sha512sum "${file_path}" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        actual_sha512=$(shasum -a 512 "${file_path}" | awk '{print $1}')
    else
        echo "ERROR: Neither sha512sum nor shasum is available to verify ${file_path}" >&2
        return 1
    fi
    if [[ "${actual_sha512}" != "${expected_sha512}" ]]; then
        echo "ERROR: SHA-512 mismatch for ${file_path}: expected ${expected_sha512}, got ${actual_sha512}" >&2
        return 1
    fi
}

function get_artifact {
    local ARTIFACT_URL=$1
    local EXPECTED_SHA512="${2:-}"
    local BASENAME
    BASENAME="$(basename "${ARTIFACT_URL}")"
    local TARGET="${E2E_TARBALL_CACHE}/${BASENAME}"

    if [ -f "${TARGET}" ] && [ -n "${EXPECTED_SHA512}" ]; then
        if ! verify_sha512 "${TARGET}" "${EXPECTED_SHA512}"; then
            echo "Removing corrupt cached artifact ${TARGET} and re-downloading" >&2
            rm -f "${TARGET}"
        fi
    fi

    if [ ! -f "${TARGET}" ]; then
        local TMP_FILE="${TARGET}.tmp.$$"
        if ! curl --fail --silent --show-error --location --retry 10 --retry-max-time 120 --output "${TMP_FILE}" "${ARTIFACT_URL}"; then
            rm -f "${TMP_FILE}"
            return 1
        fi
        if [ -n "${EXPECTED_SHA512}" ] && ! verify_sha512 "${TMP_FILE}" "${EXPECTED_SHA512}"; then
            rm -f "${TMP_FILE}"
            return 1
        fi
        mv -f "${TMP_FILE}" "${TARGET}"
    fi
    echo "${TARGET}"
}
