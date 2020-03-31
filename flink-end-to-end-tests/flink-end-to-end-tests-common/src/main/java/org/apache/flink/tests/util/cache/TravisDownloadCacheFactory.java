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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * A {@link DownloadCacheFactory} for the {@link TravisDownloadCache}.
 */
public final class TravisDownloadCacheFactory implements DownloadCacheFactory {
	private static final Logger LOG = LoggerFactory.getLogger(TravisDownloadCacheFactory.class);

	private static final ParameterProperty<Path> TMP_DIR = new ParameterProperty<>("cache-dir", value -> Paths.get(value));
	private static final ParameterProperty<Integer> BUILDS_TO_LIVE = new ParameterProperty<>("cache-btl", Integer::parseInt);
	private static final ParameterProperty<Integer> BUILD_NUMBER = new ParameterProperty<>("TRAVIS_BUILD_NUMBER", Integer::parseInt);

	@Override
	public Optional<DownloadCache> create() {
		final Optional<Path> tmpDir = TMP_DIR.get();
		final Optional<Integer> timeToLive = BUILDS_TO_LIVE.get();
		final Optional<Integer> buildNumber = BUILD_NUMBER.get();
		if (!tmpDir.isPresent()) {
			LOG.debug("Not loading {} because {} was not set.", TravisDownloadCache.class, TMP_DIR.getPropertyName());
			return Optional.empty();
		}
		if (!timeToLive.isPresent()) {
			LOG.debug("Not loading {} because {} was not set.", TravisDownloadCache.class, BUILDS_TO_LIVE.getPropertyName());
			return Optional.empty();
		}
		if (!buildNumber.isPresent()) {
			LOG.debug("Not loading {} because {} was not set.", TravisDownloadCache.class, BUILD_NUMBER.getPropertyName());
			return Optional.empty();
		}
		LOG.info("Created {}.", TravisDownloadCache.class.getSimpleName());
		return Optional.of(new TravisDownloadCache(tmpDir.get(), timeToLive.get(), buildNumber.get()));
	}
}
