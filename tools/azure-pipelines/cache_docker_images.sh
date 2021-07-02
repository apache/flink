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
echo "=============================================================================="
echo "Caching Testing Docker Images"
echo "=============================================================================="
echo ""

if [ -z "$TESTCONTAINER_CACHE_FOLDER" ]
then
      echo "\$TESTCONTAINER_CACHE_FOLDER must be set to cache the testing docker images. Exiting"
      exit 1
fi

mkdir -p "${TESTCONTAINER_CACHE_FOLDER}"

# This is the pattern that determines which containers we save.
DOCKER_IMAGE_CACHE_PATTERN="testcontainers|kafka|elasticsearch|postgres|mysql"

# The list of images in the current docker context that match the above pattern.
IMAGES_TO_CACHE=$(docker image ls --format "{{.Repository}}:{{.Tag}}" | grep -E -- ${DOCKER_IMAGE_CACHE_PATTERN})

if [ -z "$IMAGES_TO_CACHE" ]
then
      echo "No images found that match pattern (${DOCKER_IMAGE_CACHE_PATTERN}). Skipping."
else
      echo "Images To Cache"
      echo "==============="
      echo ""

      for IMAGE in ${IMAGES_TO_CACHE}
      do
          echo "${IMAGE}"
      done

      docker save ${IMAGES_TO_CACHE} -o "${TESTCONTAINER_CACHE_FOLDER}/cache.tar"
fi
