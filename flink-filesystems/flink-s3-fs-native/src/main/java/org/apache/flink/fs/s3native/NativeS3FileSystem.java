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

package org.apache.flink.fs.s3native;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.s3native.writer.NativeS3AccessHelper;
import org.apache.flink.fs.s3native.writer.NativeS3RecoverableWriter;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Native S3 FileSystem implementation using AWS SDK v2.
 *
 * <ul>
 *   <li>Operations in progress will be allowed to complete
 *   <li>New operations started after close will throw {@link IllegalStateException}
 * </ul>
 *
 * <p><b>Permission Considerations:</b> Some operations require specific IAM permissions:
 *
 * <ul>
 *   <li>{@link #getFileStatus}: Returns 403 for non-existent objects if ListBucket permission is
 *       not granted (to prevent object enumeration)
 *   <li>{@link #listStatus}: Requires ListBucket permission
 *   <li>{@link #delete}: With only DeleteObject permission, deleting non-existent objects may
 *       return errors
 * </ul>
 */
public class NativeS3FileSystem extends FileSystem
        implements EntropyInjectingFileSystem, PathsCopyingFileSystem, AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3FileSystem.class);

    /** Timeout in seconds for closing the filesystem. */
    private static final long CLOSE_TIMEOUT_SECONDS = 60;

    private final S3ClientProvider clientProvider;
    private final URI uri;
    private final String bucketName;

    @Nullable private final String entropyInjectionKey;
    private final int entropyLength;

    @Nullable private final NativeS3AccessHelper s3AccessHelper;
    private final long s3uploadPartSize;
    private final int maxConcurrentUploadsPerStream;
    private final String localTmpDir;

    @Nullable private final NativeS3BulkCopyHelper bulkCopyHelper;
    private final boolean useAsyncOperations;
    private final int readBufferSize;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public NativeS3FileSystem(
            S3ClientProvider clientProvider,
            URI uri,
            @Nullable String entropyInjectionKey,
            int entropyLength,
            String localTmpDir,
            long s3uploadPartSize,
            int maxConcurrentUploadsPerStream,
            @Nullable NativeS3BulkCopyHelper bulkCopyHelper,
            boolean useAsyncOperations,
            int readBufferSize) {
        this.clientProvider = clientProvider;
        this.uri = uri;
        this.bucketName = uri.getHost();
        this.entropyInjectionKey = entropyInjectionKey;
        this.entropyLength = entropyLength;
        this.localTmpDir = localTmpDir;
        this.s3uploadPartSize = s3uploadPartSize;
        this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
        this.useAsyncOperations = useAsyncOperations;
        this.readBufferSize = readBufferSize;
        this.s3AccessHelper =
                new NativeS3AccessHelper(
                        clientProvider.getS3Client(),
                        clientProvider.getAsyncClient(),
                        clientProvider.getTransferManager(),
                        bucketName,
                        useAsyncOperations,
                        clientProvider.getEncryptionConfig());
        this.bulkCopyHelper = bulkCopyHelper;

        if (entropyInjectionKey != null && entropyLength <= 0) {
            throw new IllegalArgumentException(
                    "Entropy length must be >= 0 when entropy injection key is set");
        }

        LOG.info(
                "Created Native S3 FileSystem for bucket: {}, entropy injection: {}, bulk copy: {}, read buffer: {} KB",
                bucketName,
                entropyInjectionKey != null,
                bulkCopyHelper != null,
                readBufferSize / 1024);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(uri);
    }

    @Override
    public Path getHomeDirectory() {
        return new Path(uri);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        checkNotClosed();
        final String key = NativeS3AccessHelper.extractKey(path);
        final S3Client s3Client = clientProvider.getS3Client();

        LOG.debug("Getting file status for s3://{}/{}", bucketName, key);

        try {
            final HeadObjectRequest request =
                    HeadObjectRequest.builder().bucket(bucketName).key(key).build();

            final HeadObjectResponse response = s3Client.headObject(request);
            final Long contentLength = response.contentLength();

            // In S3, a successful HeadObject with null/zero contentLength means
            // this is a directory marker (prefix), not an actual file
            if (contentLength == null || contentLength == 0) {
                LOG.debug(
                        "HeadObject returned null/zero content length, verifying if directory: {}",
                        key);
                return getDirectoryStatus(s3Client, key, path);
            }

            final long size = contentLength;
            final long modificationTime =
                    (response.lastModified() != null)
                            ? response.lastModified().toEpochMilli()
                            : System.currentTimeMillis();

            LOG.trace(
                    "HeadObject successful for {} - size: {}, lastModified: {}",
                    key,
                    size,
                    response.lastModified());

            return S3FileStatus.withFile(size, modificationTime, path);
        } catch (NoSuchKeyException e) {
            LOG.debug("Object not found, checking if directory: {}", key);
            return getDirectoryStatus(s3Client, key, path);
        } catch (S3Exception e) {
            final String errorCode =
                    (e.awsErrorDetails() != null) ? e.awsErrorDetails().errorCode() : "Unknown";
            final String errorMsg =
                    (e.awsErrorDetails() != null)
                            ? e.awsErrorDetails().errorMessage()
                            : e.getMessage();

            LOG.error(
                    "S3 error getting file status for s3://{}/{} - StatusCode: {}, ErrorCode: {}, Message: {}",
                    bucketName,
                    key,
                    e.statusCode(),
                    errorCode,
                    errorMsg);
            if (e.statusCode() == 403) {
                // Note: S3 returns 403 (not 404) for non-existent objects when the
                // caller lacks s3:ListBucket permission, to prevent key enumeration.
                // This 403 is therefore ambiguous: it may indicate a genuine access
                // denial OR that the object simply does not exist.
                // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
                // See:
                // https://docs.aws.amazon.com/AmazonS3/latest/userguide/troubleshoot-403-errors.html
                LOG.error(
                        "Access denied (403) for s3://{}/{}. This may indicate invalid"
                                + " credentials/bucket policy, OR the object may not exist and"
                                + " s3:ListBucket permission is not granted (S3 returns 403"
                                + " instead of 404 to prevent key enumeration).",
                        bucketName,
                        key);
            } else if (e.statusCode() == 404) {
                LOG.debug("Object not found (404) for s3://{}/{}", bucketName, key);
            }

            throw new IOException(
                    String.format(
                            "Failed to get file status for s3://%s/%s: %s",
                            bucketName, key, errorMsg),
                    e);
        }
    }

    /**
     * Checks if the given key represents a directory by listing objects with that prefix. Returns a
     * directory {@link FileStatus} if objects exist under the prefix, otherwise throws {@link
     * FileNotFoundException}.
     */
    private FileStatus getDirectoryStatus(S3Client s3Client, String key, Path path)
            throws FileNotFoundException {
        final String prefix = key.endsWith("/") ? key : key + "/";
        final ListObjectsV2Request listRequest =
                ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).maxKeys(1).build();
        final ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        if (listResponse.contents().isEmpty() && !listResponse.hasCommonPrefixes()) {
            throw new FileNotFoundException("File not found: " + path);
        }

        LOG.debug("Path is a directory: {}", key);
        return S3FileStatus.withDirectory(path);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) {
        return new BlockLocation[] {
            new S3BlockLocation(new String[] {"localhost"}, 0, file.getLen())
        };
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return open(path);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        checkNotClosed();
        final String key = NativeS3AccessHelper.extractKey(path);
        final S3Client s3Client = clientProvider.getS3Client();
        final long fileSize = getFileStatus(path).getLen();
        return new NativeS3InputStream(s3Client, bucketName, key, fileSize, readBufferSize);
    }

    /**
     * Lists the contents of a directory.
     *
     * <p><b>Retry Behavior:</b> This method relies on the AWS SDK's built-in retry mechanism. If a
     * pagination request fails after SDK retries are exhausted:
     *
     * <ul>
     *   <li>The entire operation fails with an IOException
     *   <li>Partial results from previous pages are NOT returned
     *   <li>The caller should retry the entire listStatus operation
     * </ul>
     *
     * <p>This behavior is consistent with atomic semantics - either the full listing succeeds or
     * the operation fails completely.
     */
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        checkNotClosed();
        String key = NativeS3AccessHelper.extractKey(path);
        if (!key.isEmpty() && !key.endsWith("/")) {
            key = key + "/";
        }

        final S3Client s3Client = clientProvider.getS3Client();
        final List<FileStatus> results = new ArrayList<>();
        String continuationToken = null;

        do {
            ListObjectsV2Request.Builder requestBuilder =
                    ListObjectsV2Request.builder().bucket(bucketName).prefix(key).delimiter("/");

            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }

            final ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());

            for (S3Object s3Object : response.contents()) {
                if (!s3Object.key().equals(key)) {
                    final Path objectPath =
                            new Path(uri.getScheme(), uri.getHost(), "/" + s3Object.key());
                    results.add(
                            S3FileStatus.withFile(
                                    s3Object.size(),
                                    s3Object.lastModified().toEpochMilli(),
                                    objectPath));
                }
            }

            for (software.amazon.awssdk.services.s3.model.CommonPrefix prefix :
                    response.commonPrefixes()) {
                final Path prefixPath =
                        new Path(uri.getScheme(), uri.getHost(), "/" + prefix.prefix());
                results.add(S3FileStatus.withDirectory(prefixPath));
            }

            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);

        return results.toArray(new FileStatus[0]);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        checkNotClosed();
        final String key = NativeS3AccessHelper.extractKey(path);
        final S3Client s3Client = clientProvider.getS3Client();

        try {
            final FileStatus status = getFileStatus(path);

            if (!status.isDir()) {
                final DeleteObjectRequest request =
                        DeleteObjectRequest.builder().bucket(bucketName).key(key).build();

                s3Client.deleteObject(request);
                return true;
            } else {
                if (!recursive) {
                    throw new IOException("Directory not empty and recursive = false");
                }

                final FileStatus[] contents = listStatus(path);
                for (FileStatus file : contents) {
                    delete(file.getPath(), true);
                }

                return true;
            }
        } catch (FileNotFoundException e) {
            return false;
        } catch (S3Exception e) {
            throw new IOException("Failed to delete: " + path, e);
        }
    }

    /**
     * Creates a directory at the specified path.
     *
     * <p><b>S3 Behavior:</b> S3 is a flat object store and doesn't have true directories. Directory
     * semantics are simulated through key prefixes. This method always returns true because:
     *
     * <ul>
     *   <li>S3 doesn't require directories to exist before creating objects with that prefix
     *   <li>Creating an empty "directory marker" object (key ending with /) is optional
     *   <li>Most S3 implementations don't create these markers for consistency with Hadoop FS
     * </ul>
     *
     * <p>If explicit directory markers are needed, consider using a custom implementation.
     *
     * @return always returns true (S3 doesn't require explicit directory creation)
     */
    @Override
    public boolean mkdirs(Path path) throws IOException {
        checkNotClosed();
        LOG.debug("mkdirs called for {} - S3 doesn't require explicit directory creation", path);
        return true;
    }

    @Override
    public FSDataOutputStream create(Path path, WriteMode overwriteMode) throws IOException {
        checkNotClosed();
        if (overwriteMode == WriteMode.NO_OVERWRITE) {
            try {
                if (exists(path)) {
                    throw new IOException("File already exists: " + path);
                }
            } catch (FileNotFoundException ignored) {
            }
        } else {
            try {
                delete(path, false);
            } catch (FileNotFoundException ignored) {
            }
        }

        final String key = NativeS3AccessHelper.extractKey(path);
        return new NativeS3OutputStream(
                clientProvider.getS3Client(),
                bucketName,
                key,
                localTmpDir,
                clientProvider.getEncryptionConfig());
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkNotClosed();
        final String srcKey = NativeS3AccessHelper.extractKey(src);
        final String dstKey = NativeS3AccessHelper.extractKey(dst);
        final S3Client s3Client = clientProvider.getS3Client();
        try {
            final CopyObjectRequest copyRequest =
                    CopyObjectRequest.builder()
                            .sourceBucket(bucketName)
                            .sourceKey(srcKey)
                            .destinationBucket(bucketName)
                            .destinationKey(dstKey)
                            .build();
            s3Client.copyObject(copyRequest);
            final DeleteObjectRequest deleteRequest =
                    DeleteObjectRequest.builder().bucket(bucketName).key(srcKey).build();
            s3Client.deleteObject(deleteRequest);
            return true;
        } catch (S3Exception e) {
            throw new IOException("Failed to rename " + src + " to " + dst, e);
        }
    }

    @Override
    public boolean isDistributedFS() {
        return true;
    }

    @Nullable
    @Override
    public String getEntropyInjectionKey() {
        return entropyInjectionKey;
    }

    @Override
    public String generateEntropy() {
        return StringUtils.generateRandomAlphanumericString(
                ThreadLocalRandom.current(), entropyLength);
    }

    @Override
    public boolean canCopyPaths(Path source, Path destination) {
        return bulkCopyHelper != null;
    }

    @Override
    public void copyFiles(
            List<CopyRequest> requests,
            org.apache.flink.core.fs.ICloseableRegistry closeableRegistry)
            throws IOException {
        checkNotClosed();
        if (bulkCopyHelper == null) {
            throw new UnsupportedOperationException(
                    "Bulk copy not enabled. Set s3.bulk-copy.enabled=true");
        }
        bulkCopyHelper.copyFiles(requests, closeableRegistry);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        checkNotClosed();
        if (s3AccessHelper == null) {
            throw new UnsupportedOperationException("Recoverable writer not available");
        }
        return NativeS3RecoverableWriter.writer(
                s3AccessHelper, localTmpDir, s3uploadPartSize, maxConcurrentUploadsPerStream);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }

        LOG.info("Starting async close of Native S3 FileSystem for bucket: {}", bucketName);
        return CompletableFuture.runAsync(
                        () -> {
                            if (bulkCopyHelper != null) {
                                try {
                                    bulkCopyHelper.close();
                                    LOG.debug("Bulk copy helper closed");
                                } catch (Exception e) {
                                    LOG.warn("Error closing bulk copy helper", e);
                                }
                            }

                            LOG.info("Native S3 FileSystem closed for bucket: {}", bucketName);
                        })
                .thenCompose(
                        ignored -> {
                            if (clientProvider != null) {
                                return clientProvider
                                        .closeAsync()
                                        .whenComplete(
                                                (result, error) -> {
                                                    if (error != null) {
                                                        LOG.warn(
                                                                "Error closing S3 client provider",
                                                                error);
                                                    } else {
                                                        LOG.debug("S3 client provider closed");
                                                    }
                                                });
                            }
                            return CompletableFuture.completedFuture(null);
                        })
                .orTimeout(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(
                        ex -> {
                            LOG.error(
                                    "FileSystem close timed out after {} seconds for bucket: {}",
                                    CLOSE_TIMEOUT_SECONDS,
                                    bucketName,
                                    ex);
                            return null;
                        });
    }

    /**
     * Verifies that the filesystem has not been closed.
     *
     * @throws IllegalStateException if the filesystem has been closed
     */
    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "FileSystem has been closed for bucket: "
                            + bucketName
                            + ". Operations are no longer permitted.");
        }
    }
}
