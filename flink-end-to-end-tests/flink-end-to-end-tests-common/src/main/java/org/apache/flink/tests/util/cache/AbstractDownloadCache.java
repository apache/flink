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

import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.CommandLineWrapper;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.parameters.ParameterProperty;
import org.apache.flink.util.TimeUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Stream;

/**
 * Base-class for {@link DownloadCache} implementations. This class handles the download and caching
 * of files and provides hooks for encoding/decoding a time-to-live into the file name.
 */
abstract class AbstractDownloadCache implements DownloadCache {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private static final ParameterProperty<Duration> DOWNLOAD_ATTEMPT_TIMEOUT =
            new ParameterProperty<>("cache-download-attempt-timeout", TimeUtils::parseDuration);
    private static final ParameterProperty<Duration> DOWNLOAD_GLOBAL_TIMEOUT =
            new ParameterProperty<>("cache-download-global-timeout", TimeUtils::parseDuration);
    private static final ParameterProperty<Integer> DOWNLOAD_MAX_RETRIES =
            new ParameterProperty<>("cache-download-max-retries", Integer::parseInt);

    private final Path tmpDir;
    private final Path downloadsDir;
    private final Path cacheFilesDir;

    private final Duration downloadAttemptTimeout;
    private final Duration downloadGlobalTimeout;
    private final int downloadMaxRetries;

    AbstractDownloadCache(final Path tmpDir) {
        this.tmpDir = tmpDir;
        this.downloadsDir = tmpDir.resolve("downloads");
        this.cacheFilesDir = tmpDir.resolve("cachefiles");

        this.downloadAttemptTimeout = DOWNLOAD_ATTEMPT_TIMEOUT.get(Duration.ofMinutes(2));
        this.downloadGlobalTimeout = DOWNLOAD_GLOBAL_TIMEOUT.get(Duration.ofMinutes(2));
        this.downloadMaxRetries = DOWNLOAD_MAX_RETRIES.get(3);

        log.info(
                "Download configuration: maxRetries={}, attemptTimeout={}, globalTimeout={}",
                downloadMaxRetries,
                downloadAttemptTimeout,
                downloadGlobalTimeout);
    }

    @Override
    public void before() throws IOException {
        Files.createDirectories(tmpDir);
        Files.createDirectories(downloadsDir);
        Files.createDirectories(cacheFilesDir);

        try (Stream<Path> cacheFiles = Files.list(cacheFilesDir)) {
            final Iterator<Path> iterator = cacheFiles.iterator();
            while (iterator.hasNext()) {
                final Path cacheFile = iterator.next();
                final String cacheFileName = cacheFile.getFileName().toString();

                final Matcher matcher = createCacheFileMatcher(cacheFileName);

                if (matcher.matches()) {
                    if (exceedsTimeToLive(matcher)) {
                        log.info(
                                "Invalidating cache entry {}.",
                                regenerateOriginalFileName(matcher));
                        Files.delete(cacheFile);
                    }
                }
            }
        }
    }

    @Override
    public void afterTestSuccess() {}

    abstract Matcher createCacheFileMatcher(String cacheFileName);

    abstract String generateCacheFileName(String url, String fileName);

    abstract String regenerateOriginalFileName(Matcher matcher);

    abstract boolean exceedsTimeToLive(Matcher matcher);

    abstract boolean matchesCachedFile(Matcher matcher, String url);

    @Override
    public Path getOrDownload(final String url, final Path targetDir) throws IOException {
        final Optional<Path> cachedFile = getCachedFile(url);

        final Path cacheFile;
        if (cachedFile.isPresent()) {
            cacheFile = cachedFile.get();
            log.info("Using cached version of {} from {}", url, cacheFile.toAbsolutePath());
        } else {
            final Path scopedDownloadDir = downloadsDir.resolve(String.valueOf(url.hashCode()));
            Files.createDirectories(scopedDownloadDir);
            log.info("Downloading {}.", url);
            AutoClosableProcess.create(
                            CommandLineWrapper.wget(url).targetDir(scopedDownloadDir).build())
                    .runBlockingWithRetry(
                            downloadMaxRetries, downloadAttemptTimeout, downloadGlobalTimeout);

            final Path download;
            try (Stream<Path> files = Files.list(scopedDownloadDir)) {
                final Optional<Path> any = files.findAny();
                download =
                        any.orElseThrow(() -> new IOException("Failed to download " + url + '.'));
            }

            final String cacheFileName =
                    generateCacheFileName(url, download.getFileName().toString());

            cacheFile = cacheFilesDir.resolve(cacheFileName);
            if (Files.isDirectory(download)) {
                FileUtils.moveDirectory(download.toFile(), cacheFile.toFile());
            } else {
                Files.move(download, cacheFile);
            }
        }

        final String cacheFileName = cacheFile.getFileName().toString();

        final Matcher matcher = createCacheFileMatcher(cacheFileName);
        if (!matcher.matches()) {
            // this indicates an implementation error or corrupted cache
            throw new RuntimeException(
                    "Cache file matcher did not accept file retrieved from cache.");
        }
        final String originalFileName = regenerateOriginalFileName(matcher);

        return TestUtils.copyDirectory(cacheFile, targetDir.resolve(originalFileName));
    }

    private Optional<Path> getCachedFile(final String url) throws IOException {
        try (Stream<Path> cacheFiles = Files.list(cacheFilesDir)) {
            return cacheFiles
                    .filter(
                            cacheFile -> {
                                final String cacheFileName = cacheFile.getFileName().toString();
                                final Matcher matcher = createCacheFileMatcher(cacheFileName);
                                return matcher.matches() && matchesCachedFile(matcher, url);
                            })
                    .findAny();
        }
    }
}
