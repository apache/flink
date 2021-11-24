#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#
# To reduce the number of pull requests we make against dockerhub, this script
# saves the docker images we typically pull during a testing cycle. This typically
# includes testcontainer images, kafka, elasticearch, etc.
#

if [ -z "${DOCKER_IMAGES_CACHE_FOLDER:-}" ]
then
    echo "\$DOCKER_IMAGES_CACHE_FOLDER must be set to cache the testing docker images. Exiting"
    exit 1
fi

# This is the pattern that determines which containers we save.
DOCKER_IMAGE_CACHE_PATTERN="testcontainers|kafka|elasticsearch|postgres|mysql"

# The path to the tar file that will contain the saved docker images.
DOCKER_IMAGES_CACHE_PATH="${DOCKER_IMAGES_CACHE_FOLDER}/cache.tar"

helpFunction()
{
   echo ""
   echo "Usage: $0 MODE"
   echo -e "\tMODE :: What mode to run the script in (either save or load)"
   exit 1
}

saveImages()
{
    echo "=============================================================================="
    echo "Saving Docker Images"
    echo "=============================================================================="
    echo ""

    mkdir -p "${DOCKER_IMAGES_CACHE_FOLDER}"

    # The list of images in the current docker context that match the above pattern.
    IMAGES_TO_CACHE=$(docker image ls --format "{{.Repository}}:{{.Tag}}" | grep -E -- ${DOCKER_IMAGE_CACHE_PATTERN})

    if [ -z "$IMAGES_TO_CACHE" ]
    then
          echo "No images found that match pattern (${DOCKER_IMAGE_CACHE_PATTERN}). Skipping."
    else
          echo "Images To Save"
          echo "=============="
          echo ""

          for IMAGE in ${IMAGES_TO_CACHE}
          do
              echo "${IMAGE}"
          done

          docker save ${IMAGES_TO_CACHE} -o "${DOCKER_IMAGES_CACHE_PATH}"
    fi
}

loadImages()
{
    echo "=============================================================================="
    echo "Loading Cached Docker Images"
    echo "=============================================================================="
    echo ""

    if [ -f "$DOCKER_IMAGES_CACHE_PATH" ];
    then
        docker load -i "${DOCKER_IMAGES_CACHE_PATH}"
    else
        echo "No cache file found at ${DOCKER_IMAGES_CACHE_PATH}. Skipping"
    fi
}

if [ "$1" == "save" ];
then
    saveImages
elif [ "$1" == "load" ];
then
    loadImages
else
    echo "Invalid option: ${1}"
    helpFunction
fi
