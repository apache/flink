#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#
# The Azure provided machines typically have the following disk allocation:
# Total space: 72GB
# Allocated: 56 GB
# Free: 16 GB
# This script frees up roughly 34 GB of disk space (leaving ~50 GB free) by
# deleting unneeded packages, language toolchains, preloaded Docker images and
# large directories.
# The Flink end to end tests download and generate more than 17 GB of files,
# causing unpredictable behavior and build failures.
#
# Note: the Python hosted tool cache (/opt/hostedtoolcache/Python) and the JDKs
# under /usr/lib/jvm are intentionally preserved, because downstream pipeline
# steps rely on them (UsePythonVersion task and the JAVA_HOME_<jdk>_X64 vars).
#
echo "=============================================================================="
echo "Freeing up disk space on CI system"
echo "=============================================================================="

echo "Listing 100 largest packages"
dpkg-query -Wf '${Installed-Size}\t${Package}\n' | sort -n | tail -n 100
df -h

# Package removal, directory removal and Docker pruning are independent, so they run in parallel;
# each background task logs to its own file, printed once all of them are done.
LOG_DIR="$(mktemp -d)"

echo "Removing large packages"
(
  sudo apt-get remove -y '^dotnet-.*' || true
  sudo apt-get remove -y '^llvm-.*' || true
  sudo apt-get remove -y 'php.*' || true
  sudo apt-get remove -y '^mongodb-.*' || true
  sudo apt-get remove -y '^mysql-.*' || true
  sudo apt-get remove -y '^postgresql-.*' || true
  sudo apt-get remove -y '^g\+\+-.*' '^clang-.*' '^libclang-.*' || true
  sudo apt-get remove -y '^gfortran-.*' '^libruby.*' || true
  sudo apt-get remove -y temurin-8-jdk || true
  sudo apt-get remove -y '^libllvm.*' '^libclang1.*' snapd python3-botocore podman buildah skopeo mecab-ipadic gh git-lfs || true
  sudo apt-get remove -y azure-cli google-cloud-sdk google-chrome-stable google-cloud-cli firefox microsoft-edge-stable powershell mono-devel libgl1-mesa-dri || true
  sudo apt-get autoremove -y || true
  sudo apt-get clean || true
) > "${LOG_DIR}/apt.log" 2>&1 &

echo "Removing large directories"
# Keep /opt/hostedtoolcache/Python: the e2e job's UsePythonVersion task runs
# after this script and relies on the pre-provisioned Python tool cache.
# /opt/microsoft is removed after the package removal, since the powershell package lives there.
for dir in \
    /usr/share/dotnet/ \
    /usr/local/graalvm/ \
    /usr/local/.ghcup/ \
    /usr/local/share/powershell \
    /usr/local/share/chromium \
    /usr/local/lib/android \
    /usr/local/lib/node_modules \
    /opt/hostedtoolcache/CodeQL \
    /opt/hostedtoolcache/go \
    /opt/hostedtoolcache/node \
    /opt/hostedtoolcache/Ruby \
    /opt/hostedtoolcache/PyPy \
    /usr/share/swift \
    /usr/share/miniconda \
    /opt/ghc; do
  sudo rm -rf "${dir}" &
done

echo "Pruning preloaded Docker images"
# Flink restores its cached testcontainers images later in the pipeline, so the
# preloaded base images shipped with the runner image can be removed here.
if command -v docker >/dev/null 2>&1; then
  { sudo docker system prune --all --force || true; } > "${LOG_DIR}/docker.log" 2>&1 &
fi

wait
sudo rm -rf /opt/microsoft

cat "${LOG_DIR}"/*.log
rm -rf "${LOG_DIR}"
df -h
