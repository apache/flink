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

package org.apache.flink.state.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Discovers Flink checkpoints and savepoints within a set of labelled directories.
 *
 * <p>Each configured directory is associated with a user-chosen label. By default, database names
 * are derived as {@code label/creationTs/relative-path}, where {@code creationTs} is the
 * modification time of the snapshot's {@code _metadata} file formatted as {@code
 * yyyy-MM-dd'T'HH:mm:ssX} (e.g. {@code 2026-07-22T10:30:45Z}) and {@code relative-path} is the
 * verbatim path from the configured directory to the snapshot directory (e.g. {@code
 * my-app/2026-07-22T10:30:45Z/savepoint-acce1cedsad} or {@code
 * my-app/2026-07-22T10:30:45Z/a1b2c3d4.../chk-3}). The {@code creationTs} segment can be disabled
 * via {@code dbNameIncludeTs}, in which case names fall back to {@code label/relative-path}.
 */
@Internal
class SnapshotDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotDiscovery.class);

    private static final String METADATA_FILE_NAME = "_metadata";

    private static final DateTimeFormatter CREATION_TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX").withZone(ZoneOffset.UTC);

    private final Map<String, Path> labelToDir;
    private final int listingParallelism;
    private final boolean dbNameIncludeTs;

    private ExecutorService listingExecutor;

    SnapshotDiscovery(
            Map<String, String> labelToDirPath, int listingParallelism, boolean dbNameIncludeTs) {
        this.labelToDir = validateAndConvert(labelToDirPath);
        this.listingParallelism = listingParallelism;
        this.dbNameIncludeTs = dbNameIncludeTs;
    }

    void start() {
        listingExecutor = createListingExecutor(listingParallelism);
    }

    void stop() {
        if (listingExecutor != null) {
            listingExecutor.shutdownNow();
            listingExecutor = null;
        }
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Full BFS scan of all configured directories, listing directories at the same depth
     * concurrently (up to {@code listingParallelism} at a time, with automatic backpressure via
     * {@link ThreadPoolExecutor.CallerRunsPolicy}). Returns one database name per discovered {@code
     * _metadata} file.
     *
     * @throws IOException if every configured directory fails to scan
     */
    List<String> list() throws IOException {
        List<String> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        IOException err = null;
        boolean allFailed = true;

        for (Map.Entry<String, Path> entry : labelToDir.entrySet()) {
            String label = entry.getKey();
            Path dir = entry.getValue();
            try {
                for (FileStatus metadataFileStatus : findMetadataFileStatuses(dir)) {
                    String dbName = buildDatabaseName(label, dir, metadataFileStatus);
                    if (!seen.add(dbName)) {
                        LOG.warn("Duplicate database name '{}'. Skipping.", dbName);
                        continue;
                    }
                    result.add(dbName);
                }
                allFailed = false;
            } catch (IOException e) {
                LOG.warn("Failed to scan {}: {}", dir, e.getMessage(), e);
                err = e;
            }
        }

        if (allFailed) {
            throw new IOException("All configured directories failed to scan", err);
        }
        return result;
    }

    /**
     * Reverses {@link #buildDatabaseName} to recover the label and the verbatim relative path from
     * {@code dbName}, then verifies the snapshot's {@code _metadata} file with a single {@code
     * getFileStatus} call. Returns the snapshot directory path if it exists.
     */
    Optional<String> find(String dbName) {
        if (StringUtils.isNullOrWhitespaceOnly(dbName)) {
            return Optional.empty();
        }
        int labelSlash = dbName.indexOf('/');
        if (labelSlash == 0) {
            return Optional.empty();
        }

        String label;
        String relativePath;
        if (dbNameIncludeTs) {
            // A name with no '/' has no room for the mandatory creationTs segment.
            if (labelSlash < 0) {
                return Optional.empty();
            }
            label = dbName.substring(0, labelSlash);
            String afterLabel = dbName.substring(labelSlash + 1);
            int tsSlash = afterLabel.indexOf('/');
            relativePath = tsSlash < 0 ? "" : afterLabel.substring(tsSlash + 1);
        } else {
            // A name with no '/' means the configured directory itself is the snapshot.
            label = labelSlash < 0 ? dbName : dbName.substring(0, labelSlash);
            relativePath = labelSlash < 0 ? "" : dbName.substring(labelSlash + 1);
        }

        Path dir = labelToDir.get(label);
        if (dir == null) {
            return Optional.empty();
        }

        Path metadataFile =
                relativePath.isEmpty()
                        ? new Path(dir, METADATA_FILE_NAME)
                        : new Path(dir, relativePath + "/" + METADATA_FILE_NAME);
        try {
            dir.getFileSystem().getFileStatus(metadataFile);
            return Optional.of(metadataFile.getParent().toString());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    // -------------------------------------------------------------------------
    // Full BFS scan
    // -------------------------------------------------------------------------

    private List<FileStatus> findMetadataFileStatuses(Path directory) throws IOException {
        List<FileStatus> metadataFiles = new ArrayList<>();
        List<Path> currentLevel = Collections.singletonList(directory);
        boolean allFailed = true;
        IOException err = null;

        while (!currentLevel.isEmpty()) {
            List<Future<FileStatus[]>> futures = new ArrayList<>(currentLevel.size());
            for (Path dir : currentLevel) {
                futures.add(listingExecutor.submit(() -> listDirectory(dir)));
            }

            List<Path> nextLevel = new ArrayList<>();
            for (int i = 0; i < futures.size(); i++) {
                FileStatus[] statuses = null;
                try {
                    statuses = getResult(futures.get(i));
                } catch (IOException e) {
                    LOG.warn("Failed to list {}: {}", currentLevel.get(i), e.getMessage());
                    err = e;
                }
                if (statuses == null) {
                    continue;
                }
                allFailed = false;
                for (FileStatus status : statuses) {
                    if (status.isDir()) {
                        nextLevel.add(status.getPath());
                    } else if (METADATA_FILE_NAME.equals(status.getPath().getName())) {
                        metadataFiles.add(status);
                    }
                }
            }
            currentLevel = nextLevel;
        }

        if (allFailed) {
            throw new IOException("All directory listings failed under: " + directory, err);
        }
        return metadataFiles;
    }

    private FileStatus[] listDirectory(Path dir) throws IOException {
        FileStatus[] result = dir.getFileSystem().listStatus(dir);
        if (result == null) {
            throw new IOException(
                    "Cannot list directory (path does not exist or is not a directory): " + dir);
        }
        return result;
    }

    private static FileStatus[] getResult(Future<FileStatus[]> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Directory listing failed", cause);
        }
    }

    // -------------------------------------------------------------------------
    // Database name derivation
    // -------------------------------------------------------------------------

    private String buildDatabaseName(String label, Path configuredDir, FileStatus metadataFile) {
        Path snapshotDir = metadataFile.getPath().getParent();
        String configuredPath = configuredDir.toUri().getPath();
        String snapshotPath = snapshotDir.toUri().getPath();

        String relative = snapshotPath.substring(configuredPath.length());
        if (relative.startsWith("/")) {
            relative = relative.substring(1);
        }

        StringBuilder dbName = new StringBuilder(label);
        if (dbNameIncludeTs) {
            Instant creationTs = Instant.ofEpochMilli(metadataFile.getModificationTime());
            dbName.append('/').append(CREATION_TS_FORMATTER.format(creationTs));
        }
        if (!relative.isEmpty()) {
            dbName.append('/').append(relative);
        }
        return dbName.toString();
    }

    // -------------------------------------------------------------------------
    // Construction-time helpers
    // -------------------------------------------------------------------------

    /**
     * Converts the configured label → directory paths, rejecting empty configurations, directories
     * assigned to more than one label, and directories nested inside one another (which would
     * discover the same snapshots under multiple labels).
     */
    private static Map<String, Path> validateAndConvert(Map<String, String> labelToDirPath) {
        if (labelToDirPath.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one directory must be configured via 'directory.{label}' options.");
        }

        Map<String, Path> result = new LinkedHashMap<>();
        Set<String> normalizedDirs = new LinkedHashSet<>();
        for (Map.Entry<String, String> entry : labelToDirPath.entrySet()) {
            Path dir = new Path(entry.getValue());
            if (!normalizedDirs.add(dir.toUri().getPath())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Directory '%s' is assigned to more than one label.",
                                entry.getValue()));
            }
            result.put(entry.getKey(), dir);
        }

        for (String dir : normalizedDirs) {
            String prefix = dir.endsWith("/") ? dir : dir + "/";
            for (String other : normalizedDirs) {
                if (!other.equals(dir) && other.startsWith(prefix)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Directory '%s' is an ancestor of '%s'. Providing both would "
                                            + "discover the same snapshots under multiple labels.",
                                    dir, other));
                }
            }
        }
        return result;
    }

    private static ExecutorService createListingExecutor(int parallelism) {
        return new ThreadPoolExecutor(
                parallelism,
                parallelism,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(parallelism),
                r -> {
                    Thread t = new Thread(r, "state-catalog-listing");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
