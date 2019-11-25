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

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * A {@link DownloadCacheFactory} for the {@link LolCache}.
 */
public final class LolCacheFactory implements DownloadCacheFactory {
	private static final Logger LOG = LoggerFactory.getLogger(LolCacheFactory.class);

	@Override
	public Optional<DownloadCache> create() {
		final TemporaryFolder folder = new TemporaryFolder();
		try {
			folder.create();
		} catch (IOException e) {
			throw new RuntimeException("Could not initialize temporary directory.", e);
		}
		LOG.info("Created {}.", LolCache.class.getSimpleName());
		return Optional.of(new LolCache(folder));
	}
}
