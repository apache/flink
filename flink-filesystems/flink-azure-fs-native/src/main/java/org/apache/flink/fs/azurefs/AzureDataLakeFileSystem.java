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

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.InputStreamExtension;
import org.apache.flink.core.fs.InputStreamOpener;
import org.apache.flink.core.fs.ObjectStorageInputStream;
import org.apache.flink.core.fs.ObjectStorageOutputStream;
import org.apache.flink.core.fs.OutputStreamOpener;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.WriteContext;
import org.apache.flink.core.fs.local.LocalBlockLocation;
import org.apache.flink.fs.cse.CseStreamFactory;
import org.apache.flink.fs.cse.EncryptedReadContext;
import org.apache.flink.fs.cse.EncryptedWriteContext;

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Native Azure DataLake FileSystem implementation using Azure Storage File DataLake SDK v12.
 *
 * <p>This implementation uses the DataLake (ADLS Gen2) API which provides native hierarchical
 * namespace support. Directory detection uses {@link PathProperties#isDirectory()}.
 *
 * <p>This class is thread-safe. The FileSystem instance can be shared across multiple threads.
 * Input streams returned by {@link #open} are thread-safe. Output streams returned by {@link
 * #create} are thread-safe — all public methods are guarded by a {@link
 * java.util.concurrent.locks.ReentrantLock}.
 */
@ThreadSafe
class AzureDataLakeFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(AzureDataLakeFileSystem.class);

    private static final int BYTES_PER_KB = 1024;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_PRECONDITION_FAILED = 412;

    /**
     * Azure SDK's default read chunk size (4 MB). Each HTTP GET fetches this many bytes into the
     * SDK's internal buffer. Forward seeks within this distance consume already-fetched data;
     * beyond it, reopening the stream at the target position is cheaper.
     */
    static final int SDK_READ_CHUNK_SIZE = 4 * 1024 * 1024;

    private final DataLakeStorageOperations fsClient;
    private final URI uri;
    private final String fileSystemName;
    private final int readBufferSize;
    private final long writeRequestSize;
    @Nullable private final CseStreamFactory cseFactory;
    private final boolean encryptWrites;

    private AzureDataLakeFileSystem(
            final DataLakeStorageOperations fsClient,
            final URI uri,
            final int readBufferSize,
            final long writeRequestSize,
            @Nullable final CseStreamFactory cseFactory,
            final boolean encryptWrites) {
        this.fsClient = fsClient;
        this.uri = uri;
        this.fileSystemName = getFileSystemNameFromUri(uri);
        this.readBufferSize = readBufferSize;
        this.writeRequestSize = writeRequestSize;
        this.cseFactory = cseFactory;
        this.encryptWrites = encryptWrites;

        LOG.info(
                "Created Azure DataLake FileSystem for fileSystem: {}, read buffer: {} KB, "
                        + "write request size: {} KB",
                fileSystemName,
                readBufferSize / BYTES_PER_KB,
                writeRequestSize / BYTES_PER_KB);
    }

    /**
     * Creates a builder for {@link AzureDataLakeFileSystem}.
     *
     * @param fsClient the abstraction over Azure DataLake storage operations
     * @param uri the base URI for this filesystem
     * @return a new builder
     */
    static Builder builder(final DataLakeStorageOperations fsClient, final URI uri) {
        return new Builder(fsClient, uri);
    }

    /** Builder for {@link AzureDataLakeFileSystem}. */
    static final class Builder {
        private final DataLakeStorageOperations fsClient;
        private final URI uri;
        private int readBufferSize;
        private long writeRequestSize;
        @Nullable private CseStreamFactory cseFactory;
        private boolean encryptWrites;

        private Builder(final DataLakeStorageOperations fsClient, final URI uri) {
            this.fsClient = checkNotNull(fsClient, "fsClient");
            this.uri = checkNotNull(uri, "uri");
        }

        Builder readBufferSize(final int readBufferSize) {
            this.readBufferSize = readBufferSize;
            return this;
        }

        Builder writeRequestSize(final long writeRequestSize) {
            this.writeRequestSize = writeRequestSize;
            return this;
        }

        Builder cseFactory(@Nullable final CseStreamFactory cseFactory) {
            this.cseFactory = cseFactory;
            return this;
        }

        Builder encryptWrites(final boolean encryptWrites) {
            this.encryptWrites = encryptWrites;
            return this;
        }

        AzureDataLakeFileSystem build() {
            checkArgument(readBufferSize > 0, "readBufferSize must be > 0");
            checkArgument(writeRequestSize > 0, "writeRequestSize must be > 0");
            checkArgument(
                    !encryptWrites || cseFactory != null,
                    "encryptWrites requires a CseStreamFactory");
            return new AzureDataLakeFileSystem(
                    fsClient, uri, readBufferSize, writeRequestSize, cseFactory, encryptWrites);
        }
    }

    /**
     * Extracts the container name from an abfss:// URI. The expected format is: {@code
     * abfss://container@account.dfs.core.windows.net/path}. The container name is in the userInfo
     * part (before @).
     *
     * @param uri Azure blob storage path URI
     * @return Container name extracted from URI
     * @throws IllegalArgumentException if the URI does not contain a container name in the userInfo
     */
    static String getFileSystemNameFromUri(final URI uri) {
        checkArgument(
                uri.getUserInfo() != null,
                "URI must contain a container name in the userInfo part "
                        + "(e.g., abfss://container@account.dfs.core.windows.net). Got: %s",
                uri);
        return uri.getUserInfo();
    }

    /**
     * Returns the URI that identifies this filesystem.
     *
     * @return the filesystem URI (e.g., abfss://container@account.dfs.core.windows.net)
     */
    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * Returns the current working directory for this filesystem.
     *
     * @return the base path of this filesystem
     */
    @Override
    public Path getWorkingDirectory() {
        return new Path(uri);
    }

    /**
     * Returns the home directory for this filesystem.
     *
     * @return the base path of this filesystem
     */
    @Override
    public Path getHomeDirectory() {
        return new Path(uri);
    }

    /**
     * Returns the status of a file or directory at the given path.
     *
     * <p>Uses DataLake API's native directory detection via {@link PathProperties#isDirectory()}.
     *
     * @param path the path to check
     * @return the file status containing metadata about the file or directory
     * @throws FileNotFoundException if the path does not exist
     * @throws IOException if an Azure storage error occurs
     */
    @Override
    public FileStatus getFileStatus(final Path path) throws IOException {
        final String filePath = toRelativePathString(path);

        LOG.debug("Getting file status for {}://{}/{}", uri.getScheme(), fileSystemName, filePath);

        // Handle root path
        if (filePath.isEmpty()) {
            return AzureDataLakeFileStatus.forDirectory(path, 0L);
        }

        try {
            final PathProperties properties = fsClient.getFileClient(filePath).getProperties();
            final long modificationTime =
                    ensureLastModifiedMillis(properties.getLastModified(), path.toString());

            if (Boolean.TRUE.equals(properties.isDirectory())) {
                return AzureDataLakeFileStatus.forDirectory(path, modificationTime);
            }

            final long size = properties.getFileSize();

            LOG.trace(
                    "File properties for {} - size: {}, lastModified: {}",
                    filePath,
                    size,
                    modificationTime);

            return AzureDataLakeFileStatus.forFile(path, size, size, modificationTime);
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                // With ADLS Gen2 (HNS enabled), directories are real objects.
                // If getProperties() returns 404, the path doesn't exist.
                throw new FileNotFoundException("File not found: " + path);
            }
            throw new IOException("Failed to get file status: " + path, e);
        }
    }

    /**
     * Returns the block locations for a file.
     *
     * @param file the file status to get block locations for
     * @param start the starting offset (ignored for Azure)
     * @param len the length of the region (ignored for Azure)
     * @return an array containing a single block location covering the entire file
     */
    @Override
    public BlockLocation[] getFileBlockLocations(
            final FileStatus file, final long start, final long len) {
        return new BlockLocation[] {new LocalBlockLocation(file.getLen())};
    }

    /**
     * Opens a file for reading with the specified buffer size.
     *
     * <p>If a {@link CseStreamFactory} is configured and the blob's metadata indicates it is
     * encrypted, returns a decrypting stream. Otherwise, returns a plain stream.
     *
     * @param path the path of the file to open
     * @param bufferSize the buffer size in bytes for read operations
     * @return an input stream for reading the file
     * @throws FileNotFoundException if the file does not exist
     * @throws IOException if an Azure storage error occurs
     */
    @Override
    public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
        final String filePath = toRelativePathString(path);

        try {
            final PathProperties properties = fsClient.getFileClient(filePath).getProperties();

            if (Boolean.TRUE.equals(properties.isDirectory())) {
                throw new IOException("Cannot open directory as file: " + path);
            }

            final InputStreamOpener opener =
                    AzureInputStreamFactory.createOpener(fsClient, filePath, SDK_READ_CHUNK_SIZE);
            final long fileSize = properties.getFileSize();

            if (cseFactory != null) {
                final Map<String, String> blobMetadata = properties.getMetadata();
                if (blobMetadata != null && cseFactory.isEncrypted(blobMetadata)) {
                    return cseFactory.openEncryptedRead(
                            opener,
                            EncryptedReadContext.of(blobMetadata, filePath, fileSize, bufferSize));
                }
            }

            return new ObjectStorageInputStream(
                    fileSize,
                    SDK_READ_CHUNK_SIZE, // skipOnSeekThreshold
                    InputStreamExtension.buffering(opener, bufferSize));
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                throw new FileNotFoundException("File not found: " + path);
            }
            throw new IOException("Failed to open file: " + path, e);
        }
    }

    /**
     * Opens a file for reading using the default buffer size.
     *
     * @param path the path of the file to open
     * @return an input stream for reading the file
     * @throws FileNotFoundException if the file does not exist
     * @throws IOException if an Azure storage error occurs
     */
    @Override
    public FSDataInputStream open(final Path path) throws IOException {
        return open(path, readBufferSize);
    }

    /**
     * Lists the contents of a directory.
     *
     * <p>Uses DataLake API's native directory support for accurate directory detection.
     *
     * <p>Per Flink's {@link FileSystem} contract (see {@code LocalFileSystem}): if the path refers
     * to a regular file, returns a single-element array with its status.
     *
     * @param path the directory path to list
     * @return an array of file status objects for each item in the directory
     * @throws FileNotFoundException if the path does not exist
     * @throws IOException if an Azure storage error occurs
     */
    @Override
    public FileStatus[] listStatus(final Path path) throws IOException {
        final String dirPath = toRelativePathString(path);

        // Per Flink FileSystem contract (see LocalFileSystem): if the path is a regular
        // file, return a single-element array with its status rather than throwing
        if (!dirPath.isEmpty()) {
            try {
                final PathProperties properties = fsClient.getFileClient(dirPath).getProperties();
                if (!Boolean.TRUE.equals(properties.isDirectory())) {
                    final long modTime =
                            ensureLastModifiedMillis(properties.getLastModified(), path.toString());
                    final long size = properties.getFileSize();
                    return new FileStatus[] {
                        AzureDataLakeFileStatus.forFile(path, size, size, modTime)
                    };
                }
            } catch (DataLakeStorageException e) {
                if (e.getStatusCode() == HTTP_NOT_FOUND) {
                    throw new FileNotFoundException("Path not found: " + path);
                }
                throw new IOException("Failed to get path properties: " + path, e);
            }
        }

        final List<FileStatus> results = new ArrayList<>();
        final ListPathsOptions options = new ListPathsOptions();
        // Empty dirPath means root — omitting setPath lists the root directory
        if (!dirPath.isEmpty()) {
            options.setPath(dirPath);
        }
        // Non-recursive: Flink's listStatus contract lists only immediate children
        options.setRecursive(false);

        try {
            for (final PathItem item : fsClient.listPaths(options, null)) {
                final Path itemPath = AzurePathUtils.buildDataLakePath(uri, item);
                final long modTime =
                        ensureLastModifiedMillis(item.getLastModified(), item.getName());

                if (item.isDirectory()) {
                    results.add(AzureDataLakeFileStatus.forDirectory(itemPath, modTime));
                } else {
                    final long size = item.getContentLength();
                    results.add(AzureDataLakeFileStatus.forFile(itemPath, size, size, modTime));
                }
            }
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                throw new FileNotFoundException("Directory not found: " + path);
            }
            throw new IOException("Failed to list directory: " + path, e);
        }

        // Empty result is valid: directory exists but contains no items
        return results.toArray(new FileStatus[0]);
    }

    /**
     * Deletes a file or directory at the given path.
     *
     * @param path the path to delete
     * @param recursive if true, recursively delete all contents of a directory
     * @return {@code true} if the deletion was successful, {@code false} if the path did not exist
     * @throws IOException if an error occurs
     */
    @Override
    public boolean delete(final Path path, final boolean recursive) throws IOException {
        final String filePath = toRelativePathString(path);

        if (filePath.isEmpty()) {
            throw new IOException("Cannot delete root directory");
        }

        try {
            final PathProperties properties = fsClient.getFileClient(filePath).getProperties();

            if (Boolean.TRUE.equals(properties.isDirectory())) {
                final DataLakeStorageOperations.DirectoryClient dirClient =
                        fsClient.getDirectoryClient(filePath);
                if (recursive) {
                    dirClient.deleteRecursively();
                } else {
                    try {
                        dirClient.delete();
                    } catch (DataLakeStorageException e) {
                        if (e.getStatusCode() == HTTP_CONFLICT) {
                            throw new IOException(
                                    "Directory is not empty: "
                                            + path
                                            + ". Use recursive=true to delete non-empty directories.",
                                    e);
                        }
                        throw e;
                    }
                }
            } else {
                fsClient.getFileClient(filePath).delete();
            }
            return true;
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                return false;
            }
            throw new IOException("Failed to delete: " + path, e);
        }
    }

    /**
     * Creates all directories in the given path.
     *
     * @param path the directory path to create
     * @return {@code true} if the directory was created successfully
     * @throws IOException if an error occurs
     */
    @Override
    public boolean mkdirs(final Path path) throws IOException {
        final String dirPath = toRelativePathString(path);

        // Root directory always exists in ADLS Gen2
        if (dirPath.isEmpty()) {
            return true;
        }

        try {
            fsClient.getDirectoryClient(dirPath).createIfNotExists();
            checkNotExistingFile(dirPath, path);
            return true;
        } catch (DataLakeStorageException e) {
            throw new IOException("Failed to create directory: " + path, e);
        }
    }

    /**
     * Creates a new file at the given path for writing.
     *
     * <p>For {@link WriteMode#OVERWRITE}, the SDK's {@code getOutputStream()} natively overwrites
     * existing files — no pre-delete is needed.
     *
     * <p>For {@link WriteMode#NO_OVERWRITE}, the server-side conditional write ({@code
     * If-None-Match: *}) guarantees atomic rejection when the file already exists — no client-side
     * TOCTOU window.
     *
     * <p>If {@link CseStreamFactory} is configured and {@code writeKeyId} is set, writes are
     * encrypted. When {@code writeKeyId} is absent (read-only decryption mode), writes are
     * plaintext.
     *
     * @param path the path of the file to create
     * @param overwriteMode determines behavior if the file already exists
     * @return an output stream for writing the file content
     * @throws IOException if the file already exists (NO_OVERWRITE), the path is a directory, or
     *     another error occurs
     */
    @Override
    public FSDataOutputStream create(final Path path, final WriteMode overwriteMode)
            throws IOException {
        final String filePath = toRelativePathString(path);

        try {
            final OutputStreamOpener sdkOpener =
                    AzureOutputStreamFactory.createOpener(
                            fsClient, filePath, writeRequestSize, overwriteMode);
            final OutputStream delegate;
            if (encryptWrites) {
                delegate =
                        cseFactory.openEncryptedWrite(
                                sdkOpener, EncryptedWriteContext.of(filePath));
            } else {
                delegate = sdkOpener.open(WriteContext.EMPTY_WRITE_CONTEXT);
            }
            return new ObjectStorageOutputStream(delegate, filePath);
        } catch (DataLakeStorageException e) {
            if (overwriteMode == WriteMode.NO_OVERWRITE
                    && (e.getStatusCode() == HTTP_CONFLICT
                            || e.getStatusCode() == HTTP_PRECONDITION_FAILED)) {
                throw new IOException("File already exists: " + path, e);
            }
            throw new IOException("Failed to create file: " + path, e);
        }
    }

    /**
     * Renames (moves) a file or directory from source to destination path.
     *
     * <p>Follows Hadoop semantics: if the destination is an existing directory, the source is moved
     * <em>into</em> that directory (i.e., {@code rename(src, dst)} becomes {@code rename(src,
     * dst/src.getName())}). This matches the behavior of the Hadoop-based ABFS implementation that
     * this filesystem replaces.
     *
     * <p>The destination parent directory is created if it does not exist, matching Hadoop {@code
     * FileSystem} behavior.
     *
     * <p>For directories, ADLS Gen2 performs an atomic metadata-level rename that moves the entire
     * directory tree — no per-file copying is needed.
     *
     * @param src the source path
     * @param dst the destination path
     * @return {@code true} if the rename was successful, {@code false} if the source does not exist
     * @throws IOException if an error occurs
     */
    @Override
    public boolean rename(final Path src, final Path dst) throws IOException {
        final String srcPath = toRelativePathString(src);
        String dstPath = toRelativePathString(dst);

        if (srcPath.isEmpty() || dstPath.isEmpty()) {
            throw new IOException("Cannot rename root directory");
        }

        try {
            // If dst exists, check its type (Hadoop semantics):
            // - directory → move src INTO dst
            // - file → return false (refuse to overwrite)
            try {
                final PathProperties dstProperties =
                        fsClient.getFileClient(dstPath).getProperties();
                if (Boolean.TRUE.equals(dstProperties.isDirectory())) {
                    dstPath = dstPath + "/" + src.getName();
                } else {
                    return false;
                }
            } catch (DataLakeStorageException e) {
                if (e.getStatusCode() != HTTP_NOT_FOUND) {
                    throw e;
                }
                // dst does not exist — proceed with rename as-is
            }

            // Ensure destination parent directory exists
            final int lastSlash = dstPath.lastIndexOf('/');
            if (lastSlash > 0) {
                final String parentPath = dstPath.substring(0, lastSlash);
                fsClient.getDirectoryClient(parentPath).createIfNotExists();
            }

            final DataLakeStorageOperations.FileClient fileClient = fsClient.getFileClient(srcPath);
            final PathProperties properties = fileClient.getProperties();

            if (Boolean.TRUE.equals(properties.isDirectory())) {
                fsClient.getDirectoryClient(srcPath).rename(fileSystemName, dstPath);
            } else {
                fileClient.rename(fileSystemName, dstPath);
            }
            return true;
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                return false;
            }
            throw new IOException("Failed to rename " + src + " to " + dst, e);
        }
    }

    /**
     * Returns whether this filesystem is a distributed filesystem.
     *
     * @return always {@code true}
     */
    @Override
    public boolean isDistributedFS() {
        return true;
    }

    /**
     * Recoverable writer is not supported in this iteration of the native Azure FS.
     *
     * <p>Checkpoint metadata writes fall back to {@code FSDataOutputStreamWrapper} which uses plain
     * {@code create()} + {@code close()}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public RecoverableWriter createRecoverableWriter() {
        throw new UnsupportedOperationException(
                "Recoverable writer is not supported by the native Azure filesystem. "
                        + "Checkpoint metadata uses the atomic create()+close() fallback.");
    }

    /**
     * Converts a Flink {@link Path} to a relative path string suitable for Azure SDK calls,
     * stripping the leading slash if present.
     *
     * @param path the Flink Path
     * @return the relative path string without leading slash
     */
    static String toRelativePathString(final Path path) {
        final String pathStr = path.toUri().getPath();
        if (pathStr.startsWith("/")) {
            return pathStr.substring(1);
        }
        return pathStr;
    }

    /**
     * Checks that the target path is not an existing file. ADLS Gen2 with HNS guarantees that
     * ancestors of an existing path are directories, so only the target itself needs checking.
     */
    private void checkNotExistingFile(final String dirPath, final Path path) throws IOException {
        try {
            final PathProperties props = fsClient.getFileClient(dirPath).getProperties();
            if (!Boolean.TRUE.equals(props.isDirectory())) {
                throw new IOException("Cannot create directory — path exists as a file: " + path);
            }
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                return;
            }
            throw new IOException("Failed to check path: " + path, e);
        }
    }

    /**
     * Extracts modification time in milliseconds from an Azure timestamp.
     *
     * @param lastModified the timestamp from Azure SDK
     * @param pathIdentifier identifier for the path (used in error messages)
     * @return the modification time in epoch milliseconds
     * @throws IOException if the timestamp is null
     */
    private static long ensureLastModifiedMillis(
            @Nullable final java.time.OffsetDateTime lastModified, final String pathIdentifier)
            throws IOException {
        if (lastModified == null) {
            throw new IOException(
                    "Azure Storage returned null modification time for path: " + pathIdentifier);
        }
        return lastModified.toInstant().toEpochMilli();
    }
}
