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

# legacy script for installing Hugo in CI
script_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

hugo_version="$(${script_dir}/build_docs.sh hugo-version)"
hugo_binary_arch="Linux-64bit"

hugo_artifact="hugo_extended_${hugo_version}_${hugo_binary_arch}.tar.gz"
hugo_download_url="https://github.com/gohugoio/hugo/releases/download/v${hugo_version}/${hugo_artifact}"
if ! curl --silent --fail -OL "${hugo_download_url}" ; then
  echo "Failed to download Hugo ${hugo_version} binary"
  exit 1
fi

install_dir="/usr/local/bin"
tar -zxf "${hugo_artifact}" -C "${install_dir}"
echo "[INFO] Hugo (v${hugo_version}) installed to '${install_dir}'."
rm "${hugo_artifact}"
