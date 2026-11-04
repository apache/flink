#!/bin/bash

# Deploy Hadoop 3.4.2 Test JAR to Local Repository
# This script deploys the S3 Hadoop JAR with a custom version to avoid conflicts with official releases

set -e

echo "🚀 Deploying Flink S3 Hadoop 3.4.2 Test JAR..."

# Configuration
JAR_FILE="flink-filesystems/flink-s3-fs-hadoop/target/flink-s3-fs-hadoop-1.20-SNAPSHOT.jar"
GROUP_ID="org.apache.flink"
ARTIFACT_ID="flink-s3-fs-hadoop-3.4.2-test"
VERSION="1.20-SNAPSHOT-hadoop-3.4.2-test"

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo "❌ Error: JAR file not found at $JAR_FILE"
    echo "Please run the build first: ./mvnw clean package -pl flink-filesystems/flink-s3-fs-hadoop -DskipTests"
    exit 1
fi

echo "📦 JAR file: $JAR_FILE"
echo "🏷️  Group ID: $GROUP_ID"
echo "🏷️  Artifact ID: $ARTIFACT_ID"
echo "🏷️  Version: $VERSION"

# Deploy to local repository
echo "📤 Deploying to local Maven repository..."
mvn install:install-file \
    -Dfile="$JAR_FILE" \
    -DgroupId="$GROUP_ID" \
    -DartifactId="$ARTIFACT_ID" \
    -Dversion="$VERSION" \
    -Dpackaging=jar \
    -DgeneratePom=true

echo "✅ Successfully deployed!"
echo ""
echo "📋 To use this JAR in your project, add this dependency:"
echo ""
echo "<dependency>"
echo "    <groupId>$GROUP_ID</groupId>"
echo "    <artifactId>$ARTIFACT_ID</artifactId>"
echo "    <version>$VERSION</version>"
echo "</dependency>"
echo ""
echo "🔍 JAR location in local repository:"
echo "~/.m2/repository/org/apache/flink/flink-s3-fs-hadoop-3.4.2-test/$VERSION/"
echo ""
echo "🎯 This JAR includes:"
echo "   ✅ Hadoop 3.4.2 upgrade (from 3.3.6)"
echo "   ✅ Netty conflict resolution"
echo "   ✅ S3 endpoint backward compatibility"
echo "   ✅ IAM credential provider compatibility"
echo "   ✅ Hadoop internal S3 client reflection approach"
