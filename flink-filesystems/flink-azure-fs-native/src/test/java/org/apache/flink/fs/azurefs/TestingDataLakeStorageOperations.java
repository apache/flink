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

package org.apache.flink.fs.azurefs;

import com.azure.core.http.HttpResponse;
import com.azure.storage.file.datalake.models.AccessTier;
import com.azure.storage.file.datalake.models.ArchiveStatus;
import com.azure.storage.file.datalake.models.CopyStatusType;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.LeaseDurationType;
import com.azure.storage.file.datalake.models.LeaseStateType;
import com.azure.storage.file.datalake.models.LeaseStatusType;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link DataLakeStorageOperations} for testing.
 *
 * <p>This implementation uses a {@link ConcurrentHashMap} to store path metadata and supports error
 * injection for simulating Azure Storage failures.
 */
@ThreadSafe
final class TestingDataLakeStorageOperations implements DataLakeStorageOperations {

    private static final String HDI_IS_FOLDER = "hdi_isfolder";

    private final Map<String, PathEntry> paths = new ConcurrentHashMap<>();
    private final Map<String, Integer> errorInjections = new ConcurrentHashMap<>();
    private final Map<String, Integer> listErrorInjections = new ConcurrentHashMap<>();
    private final Map<String, Integer> deleteErrorInjections = new ConcurrentHashMap<>();
    private final Map<String, Integer> createErrorInjections = new ConcurrentHashMap<>();

    private static final class PathEntry {
        final boolean isDirectory;
        @Nullable final byte[] content; // null for directories
        @Nullable final OffsetDateTime lastModified;
        @Nullable final Map<String, String> metadata;

        PathEntry(
                final boolean isDirectory,
                @Nullable final byte[] content,
                @Nullable final OffsetDateTime lastModified,
                @Nullable final Map<String, String> metadata) {
            this.isDirectory = isDirectory;
            this.content = content;
            this.lastModified = lastModified;
            this.metadata = metadata;
        }
    }

    /**
     * Adds a file to the storage.
     *
     * @param path the file path (without leading slash)
     * @param content the file content
     * @param lastModified the modification time
     * @return this instance for chaining
     */
    TestingDataLakeStorageOperations addFile(
            final String path, final byte[] content, @Nullable final OffsetDateTime lastModified) {
        return addFile(path, content, lastModified, null);
    }

    /**
     * Adds a file to the storage with optional blob metadata.
     *
     * @param path the file path (without leading slash)
     * @param content the file content
     * @param lastModified the modification time
     * @param metadata the blob metadata (e.g., encryption headers); may be {@code null}
     * @return this instance for chaining
     */
    TestingDataLakeStorageOperations addFile(
            final String path,
            final byte[] content,
            @Nullable final OffsetDateTime lastModified,
            @Nullable final Map<String, String> metadata) {
        paths.put(path, new PathEntry(false, content, lastModified, metadata));
        return this;
    }

    /**
     * Adds a directory to the storage.
     *
     * @param path the directory path (without leading slash)
     * @param lastModified the modification time
     * @return this instance for chaining
     */
    TestingDataLakeStorageOperations addDirectory(
            final String path, final OffsetDateTime lastModified) {
        paths.put(path, new PathEntry(true, null, lastModified, null));
        return this;
    }

    /**
     * Injects an error for the specified path.
     *
     * @param path the path that should fail
     * @param statusCode the HTTP status code to return
     * @return this instance for chaining
     */
    TestingDataLakeStorageOperations injectError(final String path, final int statusCode) {
        errorInjections.put(path, statusCode);
        return this;
    }

    /**
     * Injects an error for listPaths operations on the specified path.
     *
     * @param path the path that should fail during listing
     * @param statusCode the HTTP status code to return
     * @return this instance for chaining
     */
    TestingDataLakeStorageOperations injectListError(final String path, final int statusCode) {
        listErrorInjections.put(path, statusCode);
        return this;
    }

    /**
     * Injects an error for delete operations on the specified path.
     *
     * @param path the path that should fail during delete
     * @param statusCode the HTTP status code to return
     * @return this instance for chaining
     */
    TestingDataLakeStorageOperations injectDeleteError(final String path, final int statusCode) {
        deleteErrorInjections.put(path, statusCode);
        return this;
    }

    /**
     * Injects an error for create (getOutputStream) operations on the specified path.
     *
     * @param path the path that should fail during create
     * @param statusCode the HTTP status code to return
     * @return this instance for chaining
     */
    TestingDataLakeStorageOperations injectCreateError(final String path, final int statusCode) {
        createErrorInjections.put(path, statusCode);
        return this;
    }

    /** Checks if a path exists in the storage. */
    boolean exists(final String path) {
        return paths.containsKey(path);
    }

    /**
     * Creates a {@link PathProperties} instance for testing.
     *
     * <p>The SDK PathProperties constructor has 26 parameters. To mark a path as a directory,
     * metadata must contain "hdi_isfolder" = "true" (position 26 in constructor).
     */
    private static PathProperties createPathProperties(
            final boolean isDirectory,
            final long fileSize,
            @Nullable final OffsetDateTime lastModified,
            @Nullable final Map<String, String> metadata) {
        final Map<String, String> effectiveMetadata;
        if (isDirectory) {
            if (metadata != null && !metadata.isEmpty()) {
                final Map<String, String> merged = new HashMap<>(metadata);
                merged.put(HDI_IS_FOLDER, "true");
                effectiveMetadata = merged;
            } else {
                effectiveMetadata = Map.of(HDI_IS_FOLDER, "true");
            }
        } else {
            effectiveMetadata = metadata;
        }
        return new PathProperties(
                (OffsetDateTime) null, // creationTime
                lastModified,
                (String) null, // eTag
                fileSize,
                (String) null, // contentType
                (byte[]) null, // contentMd5
                (String) null, // contentEncoding
                (String) null, // contentDisposition
                (String) null, // contentLanguage
                (String) null, // cacheControl
                (LeaseStatusType) null, // leaseStatus
                (LeaseStateType) null, // leaseState
                (LeaseDurationType) null, // leaseDuration
                (String) null, // copyId
                (CopyStatusType) null, // copyStatus
                (String) null, // copySource
                (String) null, // copyProgress
                (OffsetDateTime) null, // copyCompletionTime
                (String) null, // copyStatusDescription
                (Boolean) null, // isServerEncrypted
                (Boolean) null, // isIncrementalCopy
                (AccessTier) null, // accessTier
                (ArchiveStatus) null, // archiveStatus
                (String) null, // encryptionKeySha256
                (OffsetDateTime) null, // accessTierChangeTime
                effectiveMetadata); // metadata
    }

    private static PathItem createPathItem(
            final String name,
            final boolean isDirectory,
            final long contentLength,
            @Nullable final OffsetDateTime lastModified) {
        return new PathItem(
                (String) null, // eTag
                lastModified,
                contentLength,
                (String) null, // group
                isDirectory,
                name,
                (String) null, // owner
                (String) null); // permissions
    }

    /**
     * An output stream that captures written bytes and blob metadata on close, storing them as a
     * {@link PathEntry} in the enclosing {@link TestingDataLakeStorageOperations}.
     *
     * <p>This simulates the real Azure SDK behavior where metadata is attached to the blob at write
     * time (via {@link DataLakeFileOutputStreamOptions#setMetadata}).
     */
    private final class MetadataCapturingOutputStream extends ByteArrayOutputStream {
        private final String entryPath;
        @Nullable private final Map<String, String> entryMetadata;

        MetadataCapturingOutputStream(
                final String entryPath, @Nullable final Map<String, String> entryMetadata) {
            this.entryPath = entryPath;
            this.entryMetadata = entryMetadata;
        }

        @Override
        public void close() throws IOException {
            super.close();
            paths.put(
                    entryPath,
                    new PathEntry(false, toByteArray(), OffsetDateTime.now(), entryMetadata));
        }
    }

    @Override
    public FileClient getFileClient(final String path) {
        return new FileClient() {
            @Override
            public PathProperties getProperties() throws DataLakeStorageException {
                checkError(path);
                final PathEntry entry = paths.get(path);
                if (entry == null) {
                    throw createStorageException(404, "Path not found: " + path);
                }
                final long fileSize = entry.content != null ? entry.content.length : 0L;
                return createPathProperties(
                        entry.isDirectory, fileSize, entry.lastModified, entry.metadata);
            }

            @Override
            public void delete() throws DataLakeStorageException {
                checkError(path);
                checkDeleteError(path);
                final PathEntry entry = paths.get(path);
                if (entry == null) {
                    throw createStorageException(404, "File not found: " + path);
                }
                if (entry.isDirectory) {
                    throw createStorageException(409, "Cannot delete directory as file: " + path);
                }
                paths.remove(path);
            }

            @Override
            public void rename(final String fileSystem, final String destinationPath)
                    throws DataLakeStorageException {
                checkError(path);
                final PathEntry entry = paths.get(path);
                if (entry == null) {
                    throw createStorageException(404, "Source file not found: " + path);
                }
                if (entry.isDirectory) {
                    throw createStorageException(409, "Cannot rename directory as file: " + path);
                }
                paths.remove(path);
                paths.put(destinationPath, entry);
            }

            @Override
            public OutputStream getOutputStream(final DataLakeFileOutputStreamOptions options)
                    throws DataLakeStorageException {
                checkCreateError(path);
                return new MetadataCapturingOutputStream(path, options.getMetadata());
            }

            @Override
            public InputStream openInputStream(final DataLakeFileInputStreamOptions options)
                    throws DataLakeStorageException {
                checkError(path);
                final PathEntry entry = paths.get(path);
                if (entry == null) {
                    throw createStorageException(404, "File not found: " + path);
                }
                final byte[] content = entry.content != null ? entry.content : new byte[0];
                return new ByteArrayInputStream(content);
            }
        };
    }

    @Override
    public DirectoryClient getDirectoryClient(final String path) {
        return new DirectoryClient() {
            @Override
            public void delete() throws DataLakeStorageException {
                checkError(path);
                checkDeleteError(path);
                final PathEntry entry = paths.get(path);
                if (entry == null) {
                    throw createStorageException(404, "Directory not found: " + path);
                }
                if (!entry.isDirectory) {
                    throw createStorageException(409, "Cannot delete file as directory: " + path);
                }
                // Check if directory is empty
                final String prefix = path + "/";
                for (final String p : paths.keySet()) {
                    if (p.startsWith(prefix)) {
                        throw createStorageException(409, "Directory is not empty: " + path);
                    }
                }
                paths.remove(path);
            }

            @Override
            public void deleteRecursively() throws DataLakeStorageException {
                checkError(path);
                checkDeleteError(path);
                final PathEntry entry = paths.get(path);
                if (entry == null) {
                    throw createStorageException(404, "Directory not found: " + path);
                }
                if (!entry.isDirectory) {
                    throw createStorageException(409, "Cannot delete file as directory: " + path);
                }
                // Delete the directory and all children
                final String prefix = path + "/";
                paths.keySet().removeIf(p -> p.equals(path) || p.startsWith(prefix));
            }

            @Override
            public void createIfNotExists() throws DataLakeStorageException {
                checkError(path);
                paths.putIfAbsent(path, new PathEntry(true, null, OffsetDateTime.now(), null));
            }

            @Override
            public void rename(final String fileSystem, final String destinationPath)
                    throws DataLakeStorageException {
                checkError(path);
                final PathEntry entry = paths.get(path);
                if (entry == null) {
                    throw createStorageException(404, "Source directory not found: " + path);
                }
                if (!entry.isDirectory) {
                    throw createStorageException(409, "Cannot rename file as directory: " + path);
                }

                // Rename the directory and all children
                final String srcPrefix = path + "/";
                final Map<String, PathEntry> toRename = new HashMap<>();
                for (final Map.Entry<String, PathEntry> e : paths.entrySet()) {
                    final String p = e.getKey();
                    if (p.equals(path) || p.startsWith(srcPrefix)) {
                        toRename.put(p, e.getValue());
                    }
                }

                for (final Map.Entry<String, PathEntry> e : toRename.entrySet()) {
                    final String oldPath = e.getKey();
                    final String newPath;
                    if (oldPath.equals(path)) {
                        newPath = destinationPath;
                    } else {
                        newPath = destinationPath + oldPath.substring(path.length());
                    }
                    paths.remove(oldPath);
                    paths.put(newPath, e.getValue());
                }
            }
        };
    }

    @Override
    public Iterable<PathItem> listPaths(
            final ListPathsOptions options, @Nullable final Duration timeout)
            throws DataLakeStorageException {
        final String path = options.getPath() != null ? options.getPath() : "";
        final boolean recursive = options.isRecursive();

        checkError(path);
        checkListError(path);
        final List<PathItem> results = new ArrayList<>();

        if (path.isEmpty()) {
            // List root directory
            for (final Map.Entry<String, PathEntry> entry : paths.entrySet()) {
                final String entryPath = entry.getKey();
                if (!recursive && entryPath.contains("/")) {
                    // Skip nested items in non-recursive mode
                    continue;
                }
                final PathEntry pathEntry = entry.getValue();
                final long size = pathEntry.content != null ? pathEntry.content.length : 0L;
                results.add(
                        createPathItem(
                                entryPath, pathEntry.isDirectory, size, pathEntry.lastModified));
            }
        } else {
            // List specific directory
            final String prefix = path + "/";
            for (final Map.Entry<String, PathEntry> entry : paths.entrySet()) {
                final String entryPath = entry.getKey();
                if (!entryPath.startsWith(prefix)) {
                    continue;
                }
                if (!recursive) {
                    // Non-recursive: only immediate children
                    final String relativePath = entryPath.substring(prefix.length());
                    if (relativePath.contains("/")) {
                        continue;
                    }
                }
                final PathEntry pathEntry = entry.getValue();
                final long size = pathEntry.content != null ? pathEntry.content.length : 0L;
                results.add(
                        createPathItem(
                                entryPath, pathEntry.isDirectory, size, pathEntry.lastModified));
            }
        }

        return results;
    }

    private void checkError(final String path) throws DataLakeStorageException {
        final Integer statusCode = errorInjections.get(path);
        if (statusCode != null) {
            throw createStorageException(statusCode, "Injected error for path: " + path);
        }
    }

    private void checkListError(final String path) throws DataLakeStorageException {
        final Integer statusCode = listErrorInjections.get(path);
        if (statusCode != null) {
            throw createStorageException(statusCode, "Injected list error for path: " + path);
        }
    }

    private void checkDeleteError(final String path) throws DataLakeStorageException {
        final Integer statusCode = deleteErrorInjections.get(path);
        if (statusCode != null) {
            throw createStorageException(statusCode, "Injected delete error for path: " + path);
        }
    }

    private void checkCreateError(final String path) throws DataLakeStorageException {
        final Integer statusCode = createErrorInjections.get(path);
        if (statusCode != null) {
            throw createStorageException(statusCode, "Injected create error for path: " + path);
        }
    }

    static DataLakeStorageException createStorageException(final int statusCode) {
        return createStorageException(statusCode, "test error");
    }

    static DataLakeStorageException createStorageException(
            final int statusCode, final String message) {
        return new DataLakeStorageException(message, createMockResponse(statusCode), null);
    }

    private static HttpResponse createMockResponse(final int statusCode) {
        return new HttpResponse(null) {
            @Override
            public int getStatusCode() {
                return statusCode;
            }

            @Override
            public String getHeaderValue(final String name) {
                throw new UnsupportedOperationException();
            }

            @Override
            public com.azure.core.http.HttpHeaders getHeaders() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Flux<ByteBuffer> getBody() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Mono<byte[]> getBodyAsByteArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Mono<String> getBodyAsString() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Mono<String> getBodyAsString(final Charset charset) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
