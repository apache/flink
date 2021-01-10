/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util.cache;

import org.apache.flink.tests.util.parameters.ParameterProperty;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/** A {@link DownloadCacheFactory} for the {@link TravisDownloadCache}. */
public final class TravisDownloadCacheFactory implements DownloadCacheFactory {

    private static final ParameterProperty<Path> TMP_DIR =
            new ParameterProperty<>("cache-dir", value -> Paths.get(value));
    private static final ParameterProperty<Integer> BUILDS_TO_LIVE =
            new ParameterProperty<>("cache-btl", Integer::parseInt);
    private static final ParameterProperty<Integer> BUILD_NUMBER =
            new ParameterProperty<>("TRAVIS_BUILD_NUMBER", Integer::parseInt);

    @Override
    public DownloadCache create() {
        final Optional<Path> tmpDir = TMP_DIR.get();
        final Optional<Integer> timeToLive = BUILDS_TO_LIVE.get();
        final Optional<Integer> buildNumber = BUILD_NUMBER.get();
        if (!tmpDir.isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Not loading %s because %s was not set.",
                            TravisDownloadCache.class, TMP_DIR.getPropertyName()));
        }
        if (!timeToLive.isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Not loading %s because %s was not set.",
                            TravisDownloadCache.class, BUILDS_TO_LIVE.getPropertyName()));
        }
        if (!buildNumber.isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Not loading %s because %s was not set.",
                            TravisDownloadCache.class, BUILD_NUMBER.getPropertyName()));
        }
        return new TravisDownloadCache(tmpDir.get(), timeToLive.get(), buildNumber.get());
    }
}
