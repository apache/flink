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

# Check the final fat jar for illegal or missing artifacts
check_shaded_artifacts() {
	jar tf build-target/lib/flink-dist*.jar > allClasses
	ASM=`cat allClasses | grep '^org/objectweb/asm/' | wc -l`
	if [ "$ASM" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$ASM' unshaded asm dependencies in fat jar"
		echo "=============================================================================="
		return 1
	fi

	GUAVA=`cat allClasses | grep '^com/google/common' | wc -l`
	if [ "$GUAVA" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$GUAVA' guava dependencies in fat jar"
		echo "=============================================================================="
		return 1
	fi

	CODEHAUS_JACKSON=`cat allClasses | grep '^org/codehaus/jackson' | wc -l`
	if [ "$CODEHAUS_JACKSON" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$CODEHAUS_JACKSON' unshaded org.codehaus.jackson classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	FASTERXML_JACKSON=`cat allClasses | grep '^com/fasterxml/jackson' | wc -l`
	if [ "$FASTERXML_JACKSON" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$FASTERXML_JACKSON' unshaded com.fasterxml.jackson classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	SNAPPY=`cat allClasses | grep '^org/xerial/snappy' | wc -l`
	if [ "$SNAPPY" = "0" ]; then
		echo "=============================================================================="
		echo "Missing snappy dependencies in fat jar"
		echo "=============================================================================="
		return 1
	fi

	IO_NETTY=`cat allClasses | grep '^io/netty' | wc -l`
	if [ "$IO_NETTY" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$IO_NETTY' unshaded io.netty classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	ORG_NETTY=`cat allClasses | grep '^org/jboss/netty' | wc -l`
	if [ "$ORG_NETTY" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$ORG_NETTY' unshaded org.jboss.netty classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	ZOOKEEPER=`cat allClasses | grep '^org/apache/zookeeper' | wc -l`
	if [ "$ZOOKEEPER" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$ZOOKEEPER' unshaded org.apache.zookeeper classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	CURATOR=`cat allClasses | grep '^org/apache/curator' | wc -l`
	if [ "$CURATOR" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$CURATOR' unshaded org.apache.curator classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	FLINK_PYTHON=`cat allClasses | grep '^org/apache/flink/python' | wc -l`
	if [ "$FLINK_PYTHON" != "0" ]; then
		echo "=============================================================================="
		echo "Detected that the Flink Python artifact is in the dist jar"
		echo "=============================================================================="
		return 1
	fi

	HADOOP=`cat allClasses | grep '^org/apache/hadoop' | wc -l`
	if [ "$HADOOP" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$HADOOP' Hadoop classes in the dist jar"
		echo "=============================================================================="
		return 1
	fi

	return 0
}

# Check the S3 fs implementations' fat jars for illegal or missing artifacts
check_shaded_artifacts_s3_fs() {
	VARIANT=$1
	jar tf flink-filesystems/flink-s3-fs-${VARIANT}/target/flink-s3-fs-${VARIANT}*.jar > allClasses

	if [ ! `cat allClasses | grep '^META-INF/services/org\.apache\.flink\.core\.fs\.FileSystemFactory$'` ]; then
		echo "=============================================================================="
		echo "${VARIANT}: File does not exist: services/org.apache.flink.core.fs.FileSystemFactory"
		echo "=============================================================================="
		return 1
	fi

	FS_SERVICE_FILE_CLASSES=`unzip -q -c flink-filesystems/flink-s3-fs-${VARIANT}/target/flink-s3-fs-${VARIANT}*.jar META-INF/services/org.apache.flink.core.fs.FileSystemFactory | grep -v -e '^#' -e '^$'`
	EXPECTED_FS_SERVICE_FILE_CLASSES="org.apache.flink.fs.s3${VARIANT}.S3FileSystemFactory"
	if [ "${VARIANT}" = "hadoop" ]; then
		read -r -d '' EXPECTED_FS_SERVICE_FILE_CLASSES <<EOF
org.apache.flink.fs.s3${VARIANT}.S3FileSystemFactory
org.apache.flink.fs.s3${VARIANT}.S3AFileSystemFactory
EOF
	elif [ "${VARIANT}" = "presto" ]; then
		read -r -d '' EXPECTED_FS_SERVICE_FILE_CLASSES <<EOF
org.apache.flink.fs.s3${VARIANT}.S3FileSystemFactory
org.apache.flink.fs.s3${VARIANT}.S3PFileSystemFactory
EOF
	fi

	if [ "${FS_SERVICE_FILE_CLASSES}" != "${EXPECTED_FS_SERVICE_FILE_CLASSES}" ]; then
		echo "=============================================================================="
		echo "${VARIANT}: Detected wrong content in services/org.apache.flink.core.fs.FileSystemFactory:"
		echo "${FS_SERVICE_FILE_CLASSES}"
		echo "=============================================================================="
		return 1
	fi

	return 0
}

# Check the elasticsearch connectors' fat jars for illegal or missing artifacts
check_shaded_artifacts_connector_elasticsearch() {
	VARIANT=$1
	find flink-connectors/flink-connector-elasticsearch${VARIANT}/target/flink-connector-elasticsearch${VARIANT}*.jar ! -name "*-tests.jar" -exec jar tf {} \; > allClasses

	UNSHADED_CLASSES=`cat allClasses | grep -v -e '^META-INF' -e '^assets' -e "^org/apache/flink/connector/elasticsearch/" -e "^org/apache/flink/streaming/connectors/elasticsearch/" -e "^org/apache/flink/streaming/connectors/elasticsearch${VARIANT}/" -e "^org/apache/flink/connector/elasticsearch${VARIANT}/shaded/" -e "^org/apache/flink/table/descriptors/" -e "^org/elasticsearch/" | grep '\.class$'`
	if [ "$?" = "0" ]; then
		echo "=============================================================================="
		echo "Detected unshaded dependencies in flink-connector-elasticsearch${VARIANT}'s fat jar:"
		echo "${UNSHADED_CLASSES}"
		echo "=============================================================================="
		return 1
	fi

	UNSHADED_SERVICES=`cat allClasses | grep '^META-INF/services/' | grep -v -e '^META-INF/services/org\.apache\.flink\.core\.fs\.FileSystemFactory$' -e "^META-INF/services/org\.apache\.flink\.fs\.s3${VARIANT}\.shaded" -e '^META-INF/services/'`
	if [ "$?" = "0" ]; then
		echo "=============================================================================="
		echo "Detected unshaded service files in flink-connector-elasticsearch${VARIANT}'s fat jar:"
		echo "${UNSHADED_SERVICES}"
		echo "=============================================================================="
		return 1
	fi

	return 0
}

check_one_per_package() {
    read foo
	if [ $foo -gt 1 ]
	then
		echo "ERROR - CHECK FAILED: $1 is shaded multiple times!"
		exit 1
	else
		echo "OK"
	fi
}

check_relocated() {
    read foo
	if [ $foo -ne 0 ]
	then
		echo "ERROR - CHECK FAILED: found $1 classes that where not relocated!"
		exit 1
	else
		echo "OK"
	fi
}

check_one_per_package_file_connector_base() {
  echo "Checking that flink-connector-base is included only once:"
  echo "__________________________________________________________________________"

  CONNECTOR_JARS=$(find flink-connectors -type f -name '*.jar' | grep -vE "original|connector-hive" | grep -v '\-test');
  EXIT_CODE=0

  for i in $CONNECTOR_JARS;
    do
      echo -n "- $i: ";
      jar tf $i | grep 'org/apache/flink/connector/base/source/reader/RecordEmitter' | wc -l | check_one_per_package "flink-connector-base";
      EXIT_CODE=$((EXIT_CODE+$?))
    done;
    return $EXIT_CODE;
}

check_relocated_file_connector_base() {
  echo -e "\n\n"
  echo "Checking that flink-connector-base is relocated:"
  echo "__________________________________________________________________________"

  CONNECTOR_JARS=$(find flink-connectors -type f -name '*.jar' | \
    grep -v original | grep -v '\-test' | grep -v 'flink-connectors/flink-connector-base');

  EXIT_CODE=0
  for i in $CONNECTOR_JARS;
    do
      echo -n "- $i: ";
      jar tf $i | grep '^org/apache/flink/connector/base/source/reader/RecordEmitter' | wc -l | check_relocated "flink-connector-base";
      EXIT_CODE=$((EXIT_CODE+$?))
    done;
  return $EXIT_CODE;
}



