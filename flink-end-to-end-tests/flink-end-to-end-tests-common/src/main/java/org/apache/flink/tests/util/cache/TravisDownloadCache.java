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

import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Travis-specific {@link DownloadCache} implementation that caches downloaded files in a configured directory. Cached
 * files that are older than the configured number of builds will be removed.
 *
 * @see TravisDownloadCacheFactory
 * @see TravisDownloadCacheFactory#TMP_DIR
 * @see TravisDownloadCacheFactory#BUILDS_TO_LIVE
 */
public final class TravisDownloadCache extends AbstractDownloadCache {

	private static final String CACHE_FILE_NAME_DELIMITER = "__";
	private static final Pattern CACHE_FILE_NAME_PATTERN =
		Pattern.compile("(?<hash>.*)" + CACHE_FILE_NAME_DELIMITER + "(?<build>.*)" + CACHE_FILE_NAME_DELIMITER + "(?<name>.*)");

	private final int ttl;
	private final int buildNumber;

	public TravisDownloadCache(final Path path, final int ttl, final int buildNumber) {
		super(path);
		this.ttl = ttl;
		this.buildNumber = buildNumber;
	}

	@Override
	Matcher createCacheFileMatcher(final String cacheFileName) {
		return CACHE_FILE_NAME_PATTERN.matcher(cacheFileName);
	}

	@Override
	String generateCacheFileName(final String url, final String fileName) {
		final String hash = String.valueOf(url.hashCode());

		return hash + CACHE_FILE_NAME_DELIMITER + buildNumber + CACHE_FILE_NAME_DELIMITER + fileName;
	}

	@Override
	String regenerateOriginalFileName(final Matcher matcher) {
		return matcher.group("name");
	}

	@Override
	boolean exceedsTimeToLive(final Matcher matcher) {
		int cachedBuildNumber = Integer.parseInt(matcher.group("build"));

		return buildNumber - cachedBuildNumber > ttl;
	}

	@Override
	boolean matchesCachedFile(final Matcher matcher, final String url) {
		final String hash = matcher.group("hash");

		return url.hashCode() == Integer.parseInt(hash);
	}
}
