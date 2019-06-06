#!/usr/bin/env bash
################################################################################
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
################################################################################

source "$(dirname "$0")"/common.sh

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-parent-child-classloading-test-program/target/ClassLoaderTestProgram.jar

echo "Testing parent-first class loading"

# remove any leftover classloader settings
sed -i -e 's/classloader.resolve-order: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
sed -i -e 's/classloader.parent-first-patterns: .*//' $FLINK_DIR/conf/flink-conf.yaml
echo "classloader.resolve-order: parent-first" >> "$FLINK_DIR/conf/flink-conf.yaml"

echo "Moving fake LibPackage.jar from end-to-end tests to lib/"
cp ${END_TO_END_DIR}/flink-parent-child-classloading-test-lib-package/target/LibPackage.jar ${FLINK_DIR}/lib/

function classloader_cleanup() {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  stop_cluster
  $FLINK_DIR/bin/taskmanager.sh stop-all
}
trap classloader_cleanup INT
trap classloader_cleanup EXIT

start_cluster

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --resolve-order parent-first --output $TEST_DATA_DIR/out/cl_out_pf

stop_cluster

# remove classloader settings again
sed -i -e 's/classloader.resolve-order: .*//' $FLINK_DIR/conf/flink-conf.yaml

OUTPUT=`cat $TEST_DATA_DIR/out/cl_out_pf`
# first field: whether we found the method on TaskManager
# result of getResource("parent-child-test.properties"), should be from the parent
# ordered result of getResources("parent-child-test.properties"), should have parent first
EXPECTED="Hello, from lib package!:hello-from-lib-package:hello-from-lib-packagehello-from-user-package"
if ! [[ "$OUTPUT" =~ $EXPECTED ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: $EXPECTED"
  echo -e "ACTUAL: $OUTPUT"
  exit 1
fi

# This verifies that Flink classes are still resolved from the parent because the default
# "parent-first-pattern" is "org.apache.flink"
echo "Testing child-first class loading with Flink classes loaded via parent"

# remove any leftover classloader settings
sed -i -e 's/classloader.resolve-order: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
sed -i -e 's/classloader.parent-first-patterns: .*//' $FLINK_DIR/conf/flink-conf.yaml
echo "classloader.resolve-order: child-first" >> "$FLINK_DIR/conf/flink-conf.yaml"

start_cluster

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --resolve-order parent-first --output $TEST_DATA_DIR/out/cl_out_cf_pf

stop_cluster

# remove classloader settings again
sed -i -e 's/classloader.resolve-order: .*//' $FLINK_DIR/conf/flink-conf.yaml

OUTPUT=`cat $TEST_DATA_DIR/out/cl_out_cf_pf`
# first field: whether we found the method on TaskManager
# result of getResource("parent-child-test.properties"), should be from the child
# ordered result of getResources("parent-child-test.properties"), should be child first
EXPECTED="Hello, from lib package!:hello-from-user-package:hello-from-user-packagehello-from-lib-package"
if ! [[ "$OUTPUT" =~ $EXPECTED ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: $EXPECTED"
  echo -e "ACTUAL: $OUTPUT"
  exit 1
fi

echo "Testing child-first class loading"

# remove any leftover classloader settings
sed -i -e 's/classloader.resolve-order: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
echo "classloader.resolve-order: child-first" >> "$FLINK_DIR/conf/flink-conf.yaml"
echo "classloader.parent-first-patterns: foo.bar" >> "$FLINK_DIR/conf/flink-conf.yaml"

start_cluster

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --resolve-order child-first --output $TEST_DATA_DIR/out/cl_out_cf

stop_cluster

# remove classloader settings again
sed -i -e 's/classloader.resolve-order: .*//' $FLINK_DIR/conf/flink-conf.yaml
sed -i -e 's/classloader.parent-first-patterns: .*//' $FLINK_DIR/conf/flink-conf.yaml

OUTPUT=`cat $TEST_DATA_DIR/out/cl_out_cf`
# first field: whether we found the method on TaskManager
# result of getResource("parent-child-test.properties"), should be from the child
# ordered result of getResources("parent-child-test.properties"), should be child first
EXPECTED="Hello, from user package!:hello-from-user-package:hello-from-user-packagehello-from-lib-package"
if ! [[ "$OUTPUT" =~ $EXPECTED ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: $EXPECTED"
  echo -e "ACTUAL: $OUTPUT"
  exit 1
fi
