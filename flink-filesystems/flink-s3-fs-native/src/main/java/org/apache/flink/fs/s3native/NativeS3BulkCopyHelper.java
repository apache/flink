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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.PathsCopyingFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Helper class for performing bulk S3 to local file system copies using S3TransferManager.
 *
 * <p><b>Concurrency Model:</b> Uses batch-based concurrency control with {@code
 * maxConcurrentCopies} to limit parallel downloads. The effective concurrency is clamped to the
 * HTTP connection pool size ({@code maxConnections}) to prevent connection pool exhaustion. The
 * current implementation waits for each batch to complete before starting the next batch. A future
 * enhancement could use a bounded thread pool (e.g., {@link java.util.concurrent.Semaphore} or
 * bounded executor) to allow continuous submission of new downloads as slots become available,
 * which would provide better throughput by avoiding the "slowest task in batch" bottleneck.
 *
 * <p><b>Retry Handling:</b> Relies on the S3TransferManager's built-in retry mechanism for
 * transient failures. If a download fails after retries:
 *
 * <ul>
 *   <li>The entire bulk copy operation fails with an IOException
 *   <li>Successfully downloaded files are NOT cleaned up (they remain on disk)
 *   <li>Partial downloads may leave incomplete files that should be cleaned up by the caller
 * </ul>
 *
 * <p><b>Cleanup:</b> No automatic cleanup is performed on failure. Callers are responsible for
 * cleaning up destination files if the bulk copy fails. Consider wrapping in a try-finally or using
 * a temp directory that can be deleted on failure.
 *
 * <p><b>TODO:</b> Consider extracting URI parsing logic to a shared S3UriUtils utility class to
 * consolidate S3 URI handling across the codebase.
 */
@Internal
class NativeS3BulkCopyHelper {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3BulkCopyHelper.class);

    private final S3TransferManager transferManager;
    private final int maxConcurrentCopies;
    private final int maxConnections;

    /**
     * Creates a new bulk copy helper.
     *
     * @param transferManager the S3 transfer manager for async downloads
     * @param maxConcurrentCopies the requested maximum number of concurrent copy operations
     * @param maxConnections the HTTP connection pool size; if {@code maxConcurrentCopies} exceeds
     *     this value, it is clamped down to prevent connection pool exhaustion
     */
    NativeS3BulkCopyHelper(
            S3TransferManager transferManager, int maxConcurrentCopies, int maxConnections) {
        checkArgument(maxConcurrentCopies > 0, "maxConcurrentCopies must be positive");
        checkArgument(maxConnections > 0, "maxConnections must be positive");
        this.transferManager = transferManager;
        this.maxConnections = maxConnections;
        if (maxConcurrentCopies > maxConnections) {
            LOG.warn(
                    "{} ({}) exceeds {} ({}). "
                            + "Clamping concurrent copies to {} to prevent connection pool exhaustion.",
                    NativeS3FileSystemFactory.BULK_COPY_MAX_CONCURRENT.key(),
                    maxConcurrentCopies,
                    NativeS3FileSystemFactory.MAX_CONNECTIONS.key(),
                    maxConnections,
                    maxConnections);
            this.maxConcurrentCopies = maxConnections;
        } else {
            this.maxConcurrentCopies = maxConcurrentCopies;
        }
    }

    int getMaxConcurrentCopies() {
        return maxConcurrentCopies;
    }

    /**
     * Copies files from S3 to local filesystem in batches.
     *
     * <p><b>Error Handling:</b> If an unsupported URI scheme is encountered, all already-started
     * copy operations are awaited to completion before throwing the exception. This ensures that no
     * background copy tasks are left running when the method returns, allowing the caller to safely
     * manage cleanup and resource lifecycle.
     *
     * @param requests List of copy requests (source S3 path to destination local path)
     * @param closeableRegistry Registry for cleanup (currently unused, reserved for future use)
     * @throws IOException if any copy operation fails or if an unsupported URI scheme is
     *     encountered
     */
    public void copyFiles(
            List<PathsCopyingFileSystem.CopyRequest> requests, ICloseableRegistry closeableRegistry)
            throws IOException {

        if (requests.isEmpty()) {
            return;
        }

        int totalFiles = requests.size();
        int totalBatches = (totalFiles + maxConcurrentCopies - 1) / maxConcurrentCopies;
        LOG.info(
                "Starting bulk copy of {} files using S3TransferManager "
                        + "(batch size: {}, total batches: {})",
                totalFiles,
                maxConcurrentCopies,
                totalBatches);

        List<CompletableFuture<CompletedCopy>> copyFutures = new ArrayList<>();
        int batchNumber = 0;

        try {
            for (int i = 0; i < requests.size(); i++) {
                PathsCopyingFileSystem.CopyRequest request = requests.get(i);
                String sourceUri = request.getSource().toUri().toString();
                if (sourceUri.startsWith("s3://") || sourceUri.startsWith("s3a://")) {
                    copyFutures.add(copyS3ToLocal(request));
                } else {
                    throw new UnsupportedOperationException(
                            "Only S3 to local copies are currently supported: " + sourceUri);
                }

                if (copyFutures.size() >= maxConcurrentCopies || i == requests.size() - 1) {
                    batchNumber++;
                    LOG.debug(
                            "Waiting for batch {}/{} ({} files)",
                            batchNumber,
                            totalBatches,
                            copyFutures.size());
                    waitForCopies(copyFutures);
                    copyFutures.clear();
                }
            }

            LOG.info("Completed bulk copy of {} files", totalFiles);
        } catch (Exception e) {
            if (!copyFutures.isEmpty()) {
                LOG.warn(
                        "Error during bulk copy, waiting for {} in-flight operations to complete",
                        copyFutures.size());
                try {
                    waitForCopies(copyFutures);
                } catch (IOException waitError) {
                    LOG.warn(
                            "Error waiting for in-flight copy operations: {}",
                            waitError.getMessage());
                    e.addSuppressed(waitError);
                }
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }
        }
    }

    /**
     * Initiates an async S3 to local file copy.
     *
     * @param request The copy request containing source S3 path and destination local path
     * @return A CompletableFuture that completes when the download finishes
     * @throws IOException if the destination directory cannot be created
     */
    private CompletableFuture<CompletedCopy> copyS3ToLocal(
            PathsCopyingFileSystem.CopyRequest request) throws IOException {

        String sourceUri = request.getSource().toUri().toString();
        String bucket = extractBucket(sourceUri);
        String key = extractKey(sourceUri);
        File destFile = new File(request.getDestination().getPath());

        Files.createDirectories(destFile.getParentFile().toPath());

        DownloadFileRequest downloadRequest =
                DownloadFileRequest.builder()
                        .getObjectRequest(req -> req.bucket(bucket).key(key))
                        .destination(destFile.toPath())
                        .build();

        FileDownload download = transferManager.downloadFile(downloadRequest);

        return download.completionFuture()
                .thenApply(
                        completed -> {
                            LOG.debug("Successfully copied {} to {}", sourceUri, destFile);
                            return null;
                        });
    }

    private void waitForCopies(List<CompletableFuture<CompletedCopy>> futures) throws IOException {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Bulk copy interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (isConnectionPoolExhausted(cause)) {
                throw new IOException(
                        String.format(
                                "S3 connection pool exhausted during bulk copy. "
                                        + "The configured connection pool size (%d) could not serve "
                                        + "the concurrent download requests (%d). "
                                        + "Consider reducing '%s' or increasing '%s'.",
                                maxConnections,
                                maxConcurrentCopies,
                                NativeS3FileSystemFactory.BULK_COPY_MAX_CONCURRENT.key(),
                                NativeS3FileSystemFactory.MAX_CONNECTIONS.key()),
                        cause);
            }
            throw new IOException("Bulk copy failed", cause);
        }
    }

    /**
     * Checks whether a failure was caused by HTTP connection pool exhaustion.
     *
     * <p>Walks the causal chain looking for the SDK's characteristic message about connection
     * acquire timeouts. This detection is deliberately broad (substring match on the message) to
     * remain resilient to minor SDK wording changes across versions.
     */
    @VisibleForTesting
    static boolean isConnectionPoolExhausted(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("Acquire operation took longer than")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Extracts the bucket name from an S3 URI.
     *
     * <p>Supports both s3:// and s3a:// schemes (s3a is normalized to s3).
     *
     * @param s3Uri the S3 URI
     * @return the bucket name
     */
    String extractBucket(String s3Uri) {
        String uri = s3Uri.replaceFirst("s3a://", "s3://");
        int bucketStart = uri.indexOf("://") + 3;
        int bucketEnd = uri.indexOf("/", bucketStart);
        if (bucketEnd == -1) {
            return uri.substring(bucketStart);
        }
        return uri.substring(bucketStart, bucketEnd);
    }

    /**
     * Extracts the object key from an S3 URI.
     *
     * <p>Supports both s3:// and s3a:// schemes (s3a is normalized to s3).
     *
     * @param s3Uri the S3 URI
     * @return the object key (empty string if no key in URI)
     */
    String extractKey(String s3Uri) {
        String uri = s3Uri.replaceFirst("s3a://", "s3://");
        int bucketStart = uri.indexOf("://") + 3;
        int keyStart = uri.indexOf("/", bucketStart);
        if (keyStart == -1) {
            return "";
        }
        return uri.substring(keyStart + 1);
    }
}
