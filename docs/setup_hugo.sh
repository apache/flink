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

# setup hugo
HUGO_REPO=https://github.com/gohugoio/hugo/releases/download/v0.110.0/hugo_extended_0.110.0_Linux-64bit.tar.gz
HUGO_ARTIFACT=hugo_extended_0.110.0_Linux-64bit.tar.gz
if ! curl --fail -OL $HUGO_REPO ; then
	echo "Failed to download Hugo binary"
	exit 1
fi
tar -zxvf $HUGO_ARTIFACT -C /usr/local/bin
