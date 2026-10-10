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

jarContents=/tmp/allClasses

# Check the final fat jar for illegal or missing artifacts
check_shaded_artifacts() {
	jar tf build-target/lib/flink-dist*.jar > ${jarContents}
	ASM=`cat ${jarContents} | grep '^org/objectweb/asm/' | wc -l`
	if [ "$ASM" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$ASM' unshaded asm dependencies in fat jar"
		echo "=============================================================================="
		return 1
	fi

	GUAVA=`cat ${jarContents} | grep '^com/google/common' | wc -l`
	if [ "$GUAVA" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$GUAVA' guava dependencies in fat jar"
		echo "=============================================================================="
		return 1
	fi

	CODEHAUS_JACKSON=`cat ${jarContents} | grep '^org/codehaus/jackson' | wc -l`
	if [ "$CODEHAUS_JACKSON" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$CODEHAUS_JACKSON' unshaded org.codehaus.jackson classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	FASTERXML_JACKSON=`cat ${jarContents} | grep '^com/fasterxml/jackson' | wc -l`
	if [ "$FASTERXML_JACKSON" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$FASTERXML_JACKSON' unshaded com.fasterxml.jackson classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	SNAPPY=`cat ${jarContents} | grep '^org/xerial/snappy' | wc -l`
	if [ "$SNAPPY" = "0" ]; then
		echo "=============================================================================="
		echo "Missing snappy dependencies in fat jar"
		echo "=============================================================================="
		return 1
	fi

	IO_NETTY=`cat ${jarContents} | grep '^io/netty' | wc -l`
	if [ "$IO_NETTY" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$IO_NETTY' unshaded io.netty classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	ORG_NETTY=`cat ${jarContents} | grep '^org/jboss/netty' | wc -l`
	if [ "$ORG_NETTY" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$ORG_NETTY' unshaded org.jboss.netty classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	ZOOKEEPER=`cat ${jarContents} | grep '^org/apache/zookeeper' | wc -l`
	if [ "$ZOOKEEPER" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$ZOOKEEPER' unshaded org.apache.zookeeper classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	CURATOR=`cat ${jarContents} | grep '^org/apache/curator' | wc -l`
	if [ "$CURATOR" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$CURATOR' unshaded org.apache.curator classes in fat jar"
		echo "=============================================================================="
		return 1
	fi

	FLINK_PYTHON=`cat ${jarContents} | grep '^org/apache/flink/python' | wc -l`
	if [ "$FLINK_PYTHON" != "0" ]; then
		echo "=============================================================================="
		echo "Detected that the Flink Python artifact is in the dist jar"
		echo "=============================================================================="
		return 1
	fi

	HADOOP=`cat ${jarContents} | grep '^org/apache/hadoop' | wc -l`
	if [ "$HADOOP" != "0" ]; then
		echo "=============================================================================="
		echo "Detected '$HADOOP' Hadoop classes in the dist jar"
		echo "=============================================================================="
		return 1
	fi

	return 0
}

# Check the model uber jars for dependencies that are shipped unrelocated.
# This is an allowlist rather than a denylist: anything that is not metadata and does not
# live under the module's own package fails the build, so adding a dependency without
# extending the relocations cannot go unnoticed.
check_shaded_artifacts_model() {
	VARIANT=$1

	# flink-model-openai cannot relocate com.openai or kotlin: openai-java is written in
	# Kotlin and reflects over its own classes, and both relocations break that at runtime.
	# See the relocation comment in flink-models/flink-model-openai/pom.xml.
	if [ "${VARIANT}" = "openai" ]; then
		ALLOWED='^(com/openai|kotlin)/'
	else
		ALLOWED='^$'
	fi

	# Skip the sources and javadoc jars; they sort before the shaded jar and contain no
	# classes, so picking one would make this check pass without inspecting anything.
	MODEL_JAR=`ls flink-models/flink-model-${VARIANT}/target/flink-model-${VARIANT}*.jar 2>/dev/null \
		| grep -vE -- '-(sources|javadoc|tests)\.jar$' | head -n 1`
	if [ -z "${MODEL_JAR}" ]; then
		echo "=============================================================================="
		echo "${VARIANT}: No shaded jar found, cannot verify relocations"
		echo "=============================================================================="
		return 1
	fi

	jar tf ${MODEL_JAR} > ${jarContents} || return 1

	# Guard against inspecting an empty or wrong artifact: the module's own classes must be there.
	OWN_CLASSES=`grep -c "^org/apache/flink/model/${VARIANT}/.*\.class$" ${jarContents}`
	if [ "${OWN_CLASSES}" = "0" ]; then
		echo "=============================================================================="
		echo "${VARIANT}: ${MODEL_JAR} contains no ${VARIANT} classes; wrong artifact?"
		echo "=============================================================================="
		return 1
	fi

	UNRELOCATED=`cat ${jarContents} \
		| grep -v '/$' \
		| sed -e 's#^META-INF/versions/[0-9][0-9]*/##' \
		| grep -v '^META-INF/' \
		| grep -v '^$' \
		| grep -v "^org/apache/flink/model/${VARIANT}/" \
		| grep -vE "${ALLOWED}" \
		| sed -e 's#/.*##' \
		| sort | uniq -c | sort -rn`

	if [ -n "${UNRELOCATED}" ]; then
		echo "=============================================================================="
		echo "${VARIANT}: Detected unrelocated dependencies in the model uber jar:"
		echo "${UNRELOCATED}"
		echo "=============================================================================="
		return 1
	fi

	return 0
}

# Check the S3 fs implementations' fat jars for illegal or missing artifacts
check_shaded_artifacts_s3_fs() {
	VARIANT=$1
	jar tf flink-filesystems/flink-s3-fs-${VARIANT}/target/flink-s3-fs-${VARIANT}*.jar > ${jarContents}

	if [ ! `cat ${jarContents} | grep '^META-INF/services/org\.apache\.flink\.core\.fs\.FileSystemFactory$'` ]; then
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
