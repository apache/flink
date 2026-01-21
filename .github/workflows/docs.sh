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

# override env to use Java 8 to for build
# path to JDK is taken from https://github.com/apache/flink-connector-shared-utils/blob/ci_utils/docker/base/Dockerfile#L37-L40
export JAVA_HOME=$JAVA_HOME_8_X64
export PATH=$JAVA_HOME_8_X64/bin:$PATH

mvn --version
java -version
javadoc -J-version

# workaround for a git security patch
git config --global --add safe.directory /root/flink

# Disable Maven's default HTTP blocking for legacy repos
mkdir -p ~/.m2
cat > ~/.m2/settings.xml << 'EOF'
<settings>
  <mirrors>
    <mirror>
      <id>maven-default-http-blocker</id>
      <mirrorOf>!*</mirrorOf>
      <url>http://0.0.0.0/</url>
    </mirror>
  </mirrors>
</settings>
EOF

# Jekyll docs are built in a separate step using docs/docker image
# Move pre-built docs to target/ for consistency with other branches
mv docs/content docs/target

# build Flink; required for Javadoc step
# Skip npm (node permission issues in Docker container)
mvn clean install -B -DskipTests -Dfast -Dskip.npm

# build java/scala docs
mkdir -p docs/target/api
mvn javadoc:aggregate -B \
    -Paggregate-scaladoc \
    -DadditionalJOption="-Xdoclint:none --allow-script-in-comments" \
    -Dmaven.javadoc.failOnError=false \
    -Dcheckstyle.skip=true \
    -Dspotless.check.skip=true \
    -Denforcer.skip=true \
    -Dheader="<a href=\"http://flink.apache.org/\" target=\"_top\"><h1>Back to Flink Website</h1></a> <script>var _paq=window._paq=window._paq||[];_paq.push([\"disableCookies\"]),_paq.push([\"setDomains\",[\"*.flink.apache.org\",\"*.nightlies.apache.org/flink\"]]),_paq.push([\"trackPageView\"]),_paq.push([\"enableLinkTracking\"]),function(){var u=\"//analytics.apache.org/\";_paq.push([\"setTrackerUrl\",u+\"matomo.php\"]),_paq.push([\"setSiteId\",\"1\"]);var d=document, g=d.createElement('script'), s=d.getElementsByTagName('script')[0];g.async=true; g.src=u+'matomo.js'; s.parentNode.insertBefore(g,s)}();</script>"

# Inject canonical tags into Javadoc HTML files to point to stable docs version
CANONICAL_BASE="https://nightlies.apache.org/flink/flink-docs-stable/api/java"
find target/site/apidocs -name "*.html" -type f | while read -r file; do
    REL_PATH="${file#target/site/apidocs/}"
    CANONICAL_URL="${CANONICAL_BASE}/${REL_PATH}"
    sed -i "s|<head>|<head>\n<link rel=\"canonical\" href=\"${CANONICAL_URL}\">|" "$file"
done

mv target/site/apidocs docs/target/api/java
pushd flink-scala
mvn scala:doc -B

# Inject canonical tags into Scaladoc HTML files to point to stable docs version
CANONICAL_BASE="https://nightlies.apache.org/flink/flink-docs-stable/api/scala"
find target/site/scaladocs -name "*.html" -type f | while read -r file; do
    REL_PATH="${file#target/site/scaladocs/}"
    CANONICAL_URL="${CANONICAL_BASE}/${REL_PATH}"
    sed -i "s|<head>|<head>\n<link rel=\"canonical\" href=\"${CANONICAL_URL}\">|" "$file"
done

mv target/site/scaladocs ../docs/target/api/scala
popd
