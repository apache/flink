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
set -e

mvn --version
java -version
javadoc -J-version

# setup hugo
HUGO_REPO=https://github.com/gohugoio/hugo/releases/download/v0.80.0/hugo_extended_0.80.0_Linux-64bit.tar.gz
HUGO_ARTIFACT=hugo_extended_0.80.0_Linux-64bit.tar.gz
if ! curl --fail -OL $HUGO_REPO ; then
	echo "Failed to download Hugo binary"
	exit 1
fi
tar -zxvf $HUGO_ARTIFACT
git submodule update --init --recursive
# generate docs into docs/target
./hugo -v --source docs --destination target
if [ $? -ne 0 ]; then
	echo "Error building the docs"
	exit 1
fi

# build Flink; required for Javadoc step
mvn clean install -B -DskipTests -Dfast -Pskip-webui-build

# build java/scala docs
mkdir -p docs/target/api
mvn javadoc:aggregate -B \
    -Paggregate-scaladoc \
    -DadditionalJOption="-Xdoclint:none --allow-script-in-comments" \
    -Dmaven.javadoc.failOnError=false \
    -Dcheckstyle.skip=true \
    -Dspotless.check.skip=true \
    -Denforcer.skip=true \
    -Dheader="<a href=\"http://flink.apache.org/\" target=\"_top\"><h1>Back to Flink Website</h1></a> <script>var _paq=window._paq=window._paq||[];_paq.push([\"disableCookies\"]),_paq.push([\"setDomains\",[\"*.flink.apache.org\",\"*.nightlies.apache.org/flink\"]]),_paq.push([\"trackPageView\"]),_paq.push([\"enableLinkTracking\"]),function(){var u=\"//matomo.privacy.apache.org/\";_paq.push([\"setTrackerUrl\",u+\"matomo.php\"]),_paq.push([\"setSiteId\",\"1\"]);var d=document, g=d.createElement('script'), s=d.getElementsByTagName('script')[0];g.async=true; g.src=u+'matomo.js'; s.parentNode.insertBefore(g,s)}();</script>"
mv target/site/apidocs docs/target/api/java
pushd flink-scala
mvn scala:doc -B
mv target/site/scaladocs ../docs/target/api/scala
popd

# build python docs
if [ -f  ./flink-python/dev/lint-python.sh ]; then
    # Just completely ignore sudo in conda.
    unset SUDO_UID SUDO_GID SUDO_USER

    # build python docs
    # disable the gateway, because otherwise it tries to find FLINK_HOME to access Java classes
    PYFLINK_GATEWAY_DISABLED=1 ./flink-python/dev/lint-python.sh -i "sphinx"

    # move python docs
    mv flink-python/docs/_build/html docs/target/api/python
fi
