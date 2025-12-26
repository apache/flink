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

import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link DownloadCache} implementation that does not cache anything.
 *
 * @see LolCacheFactory
 */
public final class LolCache extends AbstractDownloadCache {

    private static final Pattern CACHE_FILE_NAME_PATTERN = Pattern.compile(".*");
    private final Path tempDirectory;

    public LolCache(Path tempDirectory) {
        super(tempDirectory);
        this.tempDirectory = tempDirectory;
    }

    @Override
    public void afterTestSuccess() {
        try {
            FileUtils.deleteDirectory(tempDirectory.toFile());
        } catch (IOException e) {
            // Ignore cleanup failures
        }
    }

    @Override
    Matcher createCacheFileMatcher(String cacheFileName) {
        return CACHE_FILE_NAME_PATTERN.matcher(cacheFileName);
    }

    @Override
    String generateCacheFileName(String url, String fileName) {
        return fileName;
    }

    @Override
    String regenerateOriginalFileName(Matcher matcher) {
        return matcher.group(0);
    }

    @Override
    boolean exceedsTimeToLive(Matcher matcher) {
        return true;
    }

    @Override
    boolean matchesCachedFile(Matcher matcher, String url) {
        return false;
    }
}
