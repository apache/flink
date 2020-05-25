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

echo "Usage: ./add-jcuda-dependency.sh <cuda-version>"

CUDA_VERSION="${1:-10.0.0}"
case $(uname -s) in
  Darwin*)
  OS_NAME="apple"
  ;;
  Linux*)
  OS_NAME="linux"
  ;;
  CYGWIN*)
  OS_NAME="windows"
  ;;
  *)
  echo "Could not recognize current os" $(uname -s)
  exit 1
  ;;
esac
OS_ARCH=$(uname -m)

JCUDA_URL="http://www.jcuda.org/downloads/JCuda-All-$CUDA_VERSION.zip"
JCUDA_NATIVE_LIB="jcuda-natives-$CUDA_VERSION-$OS_NAME-$OS_ARCH.jar"
JCUBLAS_NATIVE_LIB="jcublas-natives-$CUDA_VERSION-$OS_NAME-$OS_ARCH.jar"

# Download and move the native libraries to "lib/" of Flink distribution
wget $JCUDA_URL -O jcuda.zip
unzip jcuda.zip

if [ -e ./JCuda-All-$CUDA_VERSION/$JCUDA_NATIVE_LIB ] && [ -e ./JCuda-All-$CUDA_VERSION/$JCUBLAS_NATIVE_LIB ]; then
  cp ./JCuda-All-$CUDA_VERSION/jcublas-natives-$CUDA_VERSION-$OS_NAME-$OS_ARCH.jar $(dirname "$0")/../../lib
  cp ./JCuda-All-$CUDA_VERSION/jcuda-natives-$CUDA_VERSION-$OS_NAME-$OS_ARCH.jar $(dirname "$0")/../../lib
else
  echo "Could not find the target library" $JCUDA_NATIVE_LIB "and" $JCUBLAS_NATIVE_LIB
fi

rm -f jcuda.zip
rm -rf JCuda-All-$CUDA_VERSION
