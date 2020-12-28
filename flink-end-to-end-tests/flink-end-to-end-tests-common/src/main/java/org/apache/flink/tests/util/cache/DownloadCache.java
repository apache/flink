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

import org.apache.flink.tests.util.util.FactoryUtils;
import org.apache.flink.util.ExternalResource;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A {@link DownloadCache} allows tests to download a files and/or directories and optionally caches
 * them.
 *
 * <p>Whether, how, and for how long files are cached is implementation-dependent.
 */
public interface DownloadCache extends ExternalResource {

    /**
     * Returns either a cached or newly downloaded version of the given file. The returned file path
     * is guaranteed to be located in the given target directory.
     *
     * @param url File/directory to download
     * @param targetDir directory to place file in
     * @return downloaded or cached file
     * @throws IOException if any IO operation fails
     */
    Path getOrDownload(String url, Path targetDir) throws IOException;

    /**
     * Returns the configured DownloadCache implementation, or a {@link LolCache} if none is
     * configured.
     *
     * @return configured DownloadCache, or {@link LolCache} is none is configured
     */
    static DownloadCache get() {
        return FactoryUtils.loadAndInvokeFactory(
                DownloadCacheFactory.class, DownloadCacheFactory::create, LolCacheFactory::new);
    }
}
