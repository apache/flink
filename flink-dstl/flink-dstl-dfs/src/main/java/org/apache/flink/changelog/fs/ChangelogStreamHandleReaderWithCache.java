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

package org.apache.flink.changelog.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RefCountedFile;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.changelog.fs.ChangelogStreamWrapper.wrap;
import static org.apache.flink.changelog.fs.ChangelogStreamWrapper.wrapAndSeek;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.CACHE_IDLE_TIMEOUT;

/** StateChangeIterator with local cache. */
class ChangelogStreamHandleReaderWithCache implements ChangelogStreamHandleReader {
    private static final Logger LOG =
            LoggerFactory.getLogger(ChangelogStreamHandleReaderWithCache.class);

    private static final String CACHE_FILE_SUB_DIR = "dstl-cache-file";
    private static final String CACHE_FILE_PREFIX = "dstl";

    // reference count == 1 means only cache component reference the cache file
    private static final int NO_USING_REF_COUNT = 1;

    private final File[] cacheDirectories;
    private final AtomicInteger next;

    private final ConcurrentHashMap<Path, RefCountedFile> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cacheCleanScheduler;
    private final long cacheIdleMillis;

    ChangelogStreamHandleReaderWithCache(Configuration config) {
        this.cacheDirectories =
                Arrays.stream(ConfigurationUtils.parseTempDirectories(config))
                        .map(path -> new File(path, CACHE_FILE_SUB_DIR))
                        .toArray(File[]::new);
        Arrays.stream(this.cacheDirectories).forEach(File::mkdirs);

        this.next = new AtomicInteger(new Random().nextInt(this.cacheDirectories.length));

        this.cacheCleanScheduler =
                SchedulerFactory.create(1, "ChangelogCacheFileCleanScheduler", LOG);
        this.cacheIdleMillis = config.get(CACHE_IDLE_TIMEOUT).toMillis();
    }

    @Override
    public DataInputStream openAndSeek(StreamStateHandle handle, Long offset) throws IOException {
        if (!canBeCached(handle)) {
            return wrapAndSeek(handle.openInputStream(), offset);
        }

        final FileStateHandle fileHandle = (FileStateHandle) handle;
        final RefCountedFile refCountedFile = getRefCountedFile(fileHandle);

        FileInputStream fin = openAndSeek(refCountedFile, offset);

        LOG.debug(
                "return cached file {} (rc={}) for {} (offset={})",
                refCountedFile.getFile(),
                refCountedFile.getReferenceCounter(),
                handle.getStreamStateHandleID(),
                offset);
        return wrapStream(fileHandle.getFilePath(), fin);
    }

    private boolean canBeCached(StreamStateHandle handle) throws IOException {
        if (handle instanceof FileStateHandle) {
            FileStateHandle fileHandle = (FileStateHandle) handle;
            return fileHandle.getFilePath().getFileSystem().isDistributedFS();
        } else {
            return false;
        }
    }

    private RefCountedFile getRefCountedFile(FileStateHandle fileHandle) {
        return cache.compute(
                fileHandle.getFilePath(),
                (key, oldValue) -> {
                    if (oldValue == null) {
                        oldValue = downloadToCacheFile(fileHandle);
                    }
                    oldValue.retain();
                    return oldValue;
                });
    }

    private RefCountedFile downloadToCacheFile(FileStateHandle fileHandle) {
        File directory = cacheDirectories[next.getAndIncrement() % cacheDirectories.length];
        File file;
        try {
            file = File.createTempFile(CACHE_FILE_PREFIX, null, directory);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
            return null;
        }

        try (InputStream in = wrap(fileHandle.openInputStream());
                OutputStream out = new FileOutputStream(file)) {
            LOG.debug(
                    "download and decompress dstl file : {} to cache file : {}",
                    fileHandle.getFilePath(),
                    file.getPath());
            IOUtils.copyBytes(in, out, false);

            return new RefCountedFile(file);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
            return null;
        }
    }

    private FileInputStream openAndSeek(RefCountedFile refCountedFile, long offset)
            throws IOException {
        FileInputStream fin = new FileInputStream(refCountedFile.getFile());
        if (offset != 0) {
            LOG.trace("seek to {}", offset);
            fin.getChannel().position(offset);
        }
        return fin;
    }

    private DataInputStream wrapStream(Path dfsPath, FileInputStream fin) {
        return new DataInputStream(new BufferedInputStream(fin)) {
            private boolean closed = false;

            @Override
            public void close() throws IOException {
                if (!closed) {
                    closed = true;
                    try {
                        super.close();
                    } finally {
                        cache.computeIfPresent(
                                dfsPath,
                                (key, value) -> {
                                    value.release();
                                    if (value.getReferenceCounter() == NO_USING_REF_COUNT) {
                                        cacheCleanScheduler.schedule(
                                                () -> cleanCacheFile(dfsPath),
                                                cacheIdleMillis,
                                                TimeUnit.MILLISECONDS);
                                    }
                                    return value;
                                });
                    }
                }
            }
        };
    }

    private void cleanCacheFile(Path dfsPath) {
        cache.computeIfPresent(
                dfsPath,
                (key, value) -> {
                    if (value.getReferenceCounter() == NO_USING_REF_COUNT) {
                        LOG.debug(
                                "clean cached file : {} after {}ms idle",
                                value.getFile().getPath(),
                                cacheIdleMillis);
                        value.release();
                        // remove the cache file
                        return null;
                    } else {
                        return value;
                    }
                });
    }

    @Override
    public void close() throws Exception {
        cacheCleanScheduler.shutdownNow();
        if (!cacheCleanScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.warn(
                    "Unable to cleanly shutdown cache clean scheduler of "
                            + "ChangelogHandleReaderWithCache in 5s");
        }

        Iterator<RefCountedFile> iterator = cache.values().iterator();
        while (iterator.hasNext()) {
            RefCountedFile cacheFile = iterator.next();
            iterator.remove();
            LOG.debug("cleanup on close: {}", cacheFile.getFile().toPath());
            Files.deleteIfExists(cacheFile.getFile().toPath());
        }
    }
}
