#!/bin/bash

# To clean EVERYTHING:
# mvn clean
# git clean -fdx

set -ex

FLINK_DIR=build-target
FLINK_VER=1.16.1
FLINK_BASE=flink-${FLINK_VER}
LIB_DIR="$FLINK_DIR/lib/"
OPT_DIR="$FLINK_DIR/opt/"
GIT_SHA=$(git log -n 1 --format="%H" .)

mvn install -DskipTests -Dfast -f fentik-udf
mvn install -DskipTests -Dfast -f fentik-rescale-savepoint
mvn install -DskipTests -Dfast

# Copy some required libraries that are not part of the core Flink distribution.
cp ./flink-connectors/flink-sql-connector-kafka/target/flink-sql-connector-kafka-${FLINK_VER}.jar $LIB_DIR
cp ./flink-table/flink-table-runtime/target/flink-table-runtime-${FLINK_VER}.jar  $LIB_DIR
cp ./flink-metrics/flink-metrics-prometheus/target/flink-metrics-prometheus-${FLINK_VER}.jar $LIB_DIR
cp ./fentik-udf/target/fentik-sql-functions-0.1.0.jar $LIB_DIR
cp ./fentik-rescale-savepoint/target/fentik-rescale-savepoint-0.2.0.jar $OPT_DIR

# Move flink-connector-hive jar, since it conflicts with flink-connector-files.
# mv $LIB_DIR/flink-connector-hive-${FLINK_VER}.jar $OPT_DIR

aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/libfb303-0.9.3.jar $LIB_DIR
aws s3 cp s3://dev-dataflo/ops/ec2/flink-lib/hive-exec-3.1.2.jar $LIB_DIR

mkdir -p $FLINK_DIR/plugins/s3-fs-presto
cp ./flink-filesystems/flink-s3-fs-presto/target/flink-s3-fs-presto-${FLINK_VER}.jar $FLINK_DIR/plugins/s3-fs-presto/

if [ "$1" == "--package" ]; then
    if test -n "$(git status --porcelain=v1 2>/dev/null)"; then
        echo "Uncommitted changes found, refusing to package binary."
        exit 1
    fi

    # Build a Flink binary.
    temp_dir=$(mktemp -d)
    echo "Building Flink tarball"
    # Note: we want the tarball to start with ./$FLINK_BASE/.
    mkdir $temp_dir/target
    ln -s $PWD/$FLINK_DIR $temp_dir/target/$FLINK_BASE
    pushd $temp_dir/target
    echo $GIT_SHA > $FLINK_BASE/version
    tar --exclude conf/flink-conf.yaml -zchf ../flink.tar.gz .
    popd
    S3_PATH="s3://prod-dataflo/ops/ec2/flink/$GIT_SHA"
    S3_PATH_LATEST="s3://prod-dataflo/ops/ec2/flink/latest"
    aws s3 cp $temp_dir/flink.tar.gz $S3_PATH/
    aws s3 cp $temp_dir/flink.tar.gz $S3_PATH_LATEST/
    echo $GIT_SHA | aws s3 cp - $S3_PATH_LATEST/version
    rm -rf $temp_dir
    echo "To use the new binary, update python/scripts/setup_ec2/common.sh with":
    echo "FLINK_BINARY_GIT_SHA=\"$GIT_SHA\""
fi
