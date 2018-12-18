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

CACHE_DIR="$HOME/flink_cache"
CACHE_BUILD_DIR="$CACHE_DIR/$TRAVIS_BUILD_NUMBER"
CACHE_FLINK_DIR="$CACHE_BUILD_DIR/flink"

HERE="`dirname \"$0\"`"				# relative
HERE="`( cd \"$HERE\" && pwd )`" 	# absolutized and normalized
if [ -z "$HERE" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi

source "${HERE}/travis/fold.sh"
source "${HERE}/travis/stage.sh"
source "${HERE}/travis/shade.sh"

function deleteOldCaches() {
	while read CACHE_DIR; do
		local old_number="${CACHE_DIR##*/}"
		if [ "$old_number" -lt "$TRAVIS_BUILD_NUMBER" ]; then
			echo "Deleting old cache $CACHE_DIR"
			rm -rf "$CACHE_DIR"
		fi
	done
}

# delete leftover caches from previous builds
find "$CACHE_DIR" -mindepth 1 -maxdepth 1 | grep -v "$TRAVIS_BUILD_NUMBER" | deleteOldCaches

function getCurrentStage() {
	STAGE_NUMBER=$(echo "$TRAVIS_JOB_NUMBER" | cut -d'.' -f 2)
	case $STAGE_NUMBER in
		(1)
			echo "$STAGE_COMPILE"
			;;
		(2)
			echo "$STAGE_CORE"
			;;
		(3)
			echo "$STAGE_LIBRARIES"
			;;
		(4)
			echo "$STAGE_CONNECTORS"
			;;
		(5)
			echo "$STAGE_TESTS"
			;;
		(6)
			echo "$STAGE_MISC"
			;;
		(7)
			echo "$STAGE_CLEANUP"
			;;
		(*)
			echo "Invalid stage detected ($STAGE_NUMBER)"
			return 1
			;;
	esac

	return 0
}

STAGE=$(getCurrentStage)
if [ $? != 0 ]; then
	echo "Could not determine current stage."
	exit 1
fi
echo "Current stage: \"$STAGE\""

EXIT_CODE=0

# Run actual compile&test steps
if [ $STAGE == "$STAGE_COMPILE" ]; then
	MVN="mvn clean install -nsu -Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -Dmaven.javadoc.skip=true -B -DskipTests $PROFILE"
	$MVN
	EXIT_CODE=$?

    if [ $EXIT_CODE == 0 ]; then
        printf "\n\n==============================================================================\n"
        printf "Checking scala suffixes\n"
        printf "==============================================================================\n"

        ./tools/verify_scala_suffixes.sh
        EXIT_CODE=$?
    else
        printf "\n==============================================================================\n"
        printf "Previous build failure detected, skipping scala-suffixes check.\n"
        printf "==============================================================================\n"
    fi

    if [ $EXIT_CODE == 0 ]; then
        printf "\n\n==============================================================================\n"
        printf "Checking dependency convergence\n"
        printf "==============================================================================\n"

        ./tools/check_dependency_convergence.sh
        EXIT_CODE=$?
    else
        printf "\n==============================================================================\n"
        printf "Previous build failure detected, skipping dependency-convergence check.\n"
        printf "==============================================================================\n"
    fi
    
    if [ $EXIT_CODE == 0 ]; then
        check_shaded_artifacts
        EXIT_CODE=$(($EXIT_CODE+$?))
        check_shaded_artifacts_s3_fs hadoop
        EXIT_CODE=$(($EXIT_CODE+$?))
        check_shaded_artifacts_s3_fs presto
        EXIT_CODE=$(($EXIT_CODE+$?))
        check_shaded_artifacts_connector_elasticsearch ""
        EXIT_CODE=$(($EXIT_CODE+$?))
        check_shaded_artifacts_connector_elasticsearch 2
        EXIT_CODE=$(($EXIT_CODE+$?))
        check_shaded_artifacts_connector_elasticsearch 5
        EXIT_CODE=$(($EXIT_CODE+$?))
    else
        echo "=============================================================================="
        echo "Previous build failure detected, skipping shaded dependency check."
        echo "=============================================================================="
    fi

    if [ $EXIT_CODE == 0 ]; then
        echo "Creating cache build directory $CACHE_FLINK_DIR"
        mkdir -p "$CACHE_FLINK_DIR"
    
        cp -r . "$CACHE_FLINK_DIR"

        function minimizeCachedFiles() {
            # reduces the size of the cached directory to speed up
            # the packing&upload / download&unpacking process
            # by removing files not required for subsequent stages
    
            # original jars
            find "$CACHE_FLINK_DIR" -maxdepth 8 -type f -name 'original-*.jar' | xargs rm -rf
    
            # .git directory
            # not deleting this can cause build stability issues
            # merging the cached version sometimes fails
            rm -rf "$CACHE_FLINK_DIR/.git"
        }
    
        start_fold "minimize_cache" "Minimizing cache"
        travis_time_start
        minimizeCachedFiles
        travis_time_finish
        end_fold "minimize_cache"
    else
        echo "=============================================================================="
        echo "Previous build failure detected, skipping cache setup."
        echo "=============================================================================="
    fi
elif [ $STAGE != "$STAGE_CLEANUP" ]; then
	if ! [ -e $CACHE_FLINK_DIR ]; then
		echo "Cached flink dir $CACHE_FLINK_DIR does not exist. Exiting build."
		exit 1
	fi
	# merged compiled flink into local clone
	# this prevents the cache from being re-uploaded
	start_fold "merge_cache" "Merging cache"
	travis_time_start
	cp -RT "$CACHE_FLINK_DIR" "."
	travis_time_finish
	end_fold "merge_cache"

	start_fold "adjust_timestamps" "Adjusting timestamps"
	travis_time_start
	# adjust timestamps to prevent recompilation
	find . -type f -name '*.java' | xargs touch
	find . -type f -name '*.scala' | xargs touch
	find . -type f -name '*.class' | xargs touch
	find . -type f -name '*.timestamp' | xargs touch
	travis_time_finish
	end_fold "adjust_timestamps"

	TEST="$STAGE" "./tools/travis_mvn_watchdog.sh" 300
	EXIT_CODE=$?
else
	echo "Cleaning up $CACHE_BUILD_DIR"
	rm -rf "$CACHE_BUILD_DIR"
fi

# Exit code for Travis build success/failure
exit $EXIT_CODE
