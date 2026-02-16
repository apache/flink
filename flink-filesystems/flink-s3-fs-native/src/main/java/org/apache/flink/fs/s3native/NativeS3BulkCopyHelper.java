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

/**
 * Helper class for performing bulk S3 to local file system copies using S3TransferManager.
 *
 * <p><b>Concurrency Model:</b> Uses batch-based concurrency control with {@code
 * maxConcurrentCopies} to limit parallel downloads. The current implementation waits for each batch
 * to complete before starting the next batch. A future enhancement could use a bounded thread pool
 * (e.g., {@link java.util.concurrent.Semaphore} or bounded executor) to allow continuous submission
 * of new downloads as slots become available, which would provide better throughput by avoiding the
 * "slowest task in batch" bottleneck.
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
public class NativeS3BulkCopyHelper {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3BulkCopyHelper.class);

    private final S3TransferManager transferManager;
    private final int maxConcurrentCopies;

    public NativeS3BulkCopyHelper(S3TransferManager transferManager, int maxConcurrentCopies) {
        this.transferManager = transferManager;
        this.maxConcurrentCopies = maxConcurrentCopies;
    }

    /**
     * Copies files from S3 to local filesystem in batches.
     *
     * @param requests List of copy requests (source S3 path to destination local path)
     * @param closeableRegistry Registry for cleanup (currently unused, reserved for future use)
     * @throws IOException if any copy operation fails
     */
    public void copyFiles(
            List<PathsCopyingFileSystem.CopyRequest> requests, ICloseableRegistry closeableRegistry)
            throws IOException {

        if (requests.isEmpty()) {
            return;
        }

        LOG.info("Starting bulk copy of {} files using S3TransferManager", requests.size());

        List<CompletableFuture<CompletedCopy>> copyFutures = new ArrayList<>();

        for (int i = 0; i < requests.size(); i++) {
            PathsCopyingFileSystem.CopyRequest request = requests.get(i);
            String sourceUri = request.getSource().toUri().toString();
            if (sourceUri.startsWith("s3://") || sourceUri.startsWith("s3a://")) {
                CompletableFuture<CompletedCopy> future = copyS3ToLocal(request);
                copyFutures.add(future);
            } else {
                throw new UnsupportedOperationException(
                        "Only S3 to local copies are currently supported: " + sourceUri);
            }

            if (copyFutures.size() >= maxConcurrentCopies || i == requests.size() - 1) {
                waitForCopies(copyFutures);
                copyFutures.clear();
            }
        }

        LOG.info("Completed bulk copy of {} files", requests.size());
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
            throw new IOException("Bulk copy failed", e.getCause());
        }
    }

    // TODO: Consider moving these URI parsing methods to a shared S3UriUtils class

    /**
     * Extracts the bucket name from an S3 URI.
     *
     * <p>Supports both s3:// and s3a:// schemes (s3a is normalized to s3).
     */
    private String extractBucket(String s3Uri) {
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
     */
    private String extractKey(String s3Uri) {
        String uri = s3Uri.replaceFirst("s3a://", "s3://");
        int bucketStart = uri.indexOf("://") + 3;
        int keyStart = uri.indexOf("/", bucketStart);
        if (keyStart == -1) {
            return "";
        }
        return uri.substring(keyStart + 1);
    }
}
