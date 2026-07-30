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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Helper class for performing bulk S3 to local file system copies using the S3 async client.
 *
 * <p><b>Concurrency Model:</b> Uses batch-based concurrency control with {@code
 * maxConcurrentCopies} to limit parallel downloads. The effective concurrency is clamped to the
 * HTTP connection pool size ({@code maxConnections}) to prevent connection pool exhaustion. The
 * current implementation waits for each batch to complete before starting the next batch. A future
 * enhancement could use a bounded thread pool (e.g., {@link java.util.concurrent.Semaphore} or
 * bounded executor) to allow continuous submission of new downloads as slots become available,
 * which would provide better throughput by avoiding the "slowest task in batch" bottleneck.
 *
 * <p><b>Retry Handling:</b> Relies on the S3 async client's built-in retry mechanism for transient
 * failures. If a download fails after retries:
 *
 * <ul>
 *   <li>The entire bulk copy operation fails with an IOException
 *   <li>Successfully downloaded files are NOT cleaned up (they remain on disk)
 *   <li>The failed file's temporary download is deleted before the method returns
 * </ul>
 *
 * <p><b>Cleanup:</b> Each file is downloaded into a temporary file in the destination directory and
 * atomically moved into place after the stream has been fully copied. Cancellation through the
 * provided {@link ICloseableRegistry} aborts active S3 response streams, cancels pending futures,
 * stops the worker pool, and deletes incomplete temporary files.
 *
 * <p><b>TODO:</b> Consider extracting URI parsing logic to a shared S3UriUtils utility class to
 * consolidate S3 URI handling across the codebase.
 */
@Internal
class NativeS3BulkCopyHelper {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3BulkCopyHelper.class);

    private final S3AsyncClient asyncClient;
    private final int maxConcurrentCopies;
    private final int maxConnections;
    private final int downloadBufferSize;

    /**
     * Creates a new bulk copy helper.
     *
     * @param asyncClient the S3 async client used for downloads
     * @param maxConcurrentCopies the requested maximum number of concurrent copy operations
     * @param maxConnections the HTTP connection pool size; if {@code maxConcurrentCopies} exceeds
     *     this value, it is clamped down to prevent connection pool exhaustion
     * @param downloadBufferSize the buffer size in bytes used to write each downloaded file to the
     *     local filesystem; bounds the temporary direct buffers the JDK caches for channel writes
     */
    NativeS3BulkCopyHelper(
            S3AsyncClient asyncClient,
            int maxConcurrentCopies,
            int maxConnections,
            int downloadBufferSize) {
        checkArgument(maxConcurrentCopies > 0, "maxConcurrentCopies must be positive");
        checkArgument(maxConnections > 0, "maxConnections must be positive");
        checkArgument(downloadBufferSize > 0, "downloadBufferSize must be positive");
        this.asyncClient = asyncClient;
        this.maxConnections = maxConnections;
        this.downloadBufferSize = downloadBufferSize;
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

    @VisibleForTesting
    int getDownloadBufferSize() {
        return downloadBufferSize;
    }

    /**
     * Copies files from S3 to local filesystem in batches.
     *
     * <p><b>Error Handling:</b> If an unsupported URI scheme or copy failure is encountered,
     * already-started copy operations are cancelled before throwing the exception. Successfully
     * completed destination files are left in place; incomplete temporary files are deleted.
     *
     * @param requests List of copy requests (source S3 path to destination local path)
     * @param closeableRegistry Registry for cancelling in-flight copies during task cancellation
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
                "Starting bulk copy of {} files (batch size: {}, total batches: {})",
                totalFiles,
                maxConcurrentCopies,
                totalBatches);

        ExecutorService downloadPool =
                Executors.newFixedThreadPool(
                        maxConcurrentCopies,
                        new ExecutorThreadFactory(
                                "s3-native-bulk-copy",
                                (thread, error) ->
                                        LOG.error(
                                                "Uncaught exception in S3 bulk-copy worker {}",
                                                thread.getName(),
                                                error)));
        BulkCopyCancellation cancellation = new BulkCopyCancellation(downloadPool);
        ICloseableRegistry registry =
                closeableRegistry == null ? ICloseableRegistry.NO_OP : closeableRegistry;
        List<CompletableFuture<Void>> copyFutures = new ArrayList<>();
        int batchNumber = 0;

        try (Closeable ignored = registry.registerCloseableTemporarily(cancellation)) {
            for (int i = 0; i < requests.size(); i++) {
                PathsCopyingFileSystem.CopyRequest request = requests.get(i);
                String sourceUri = request.getSource().toUri().toString();
                if (isSupportedS3Scheme(request.getSource())
                        && isSupportedLocalScheme(request.getDestination())) {
                    copyFutures.add(copyS3ToLocal(request, downloadPool, cancellation));
                } else {
                    throw new UnsupportedOperationException(
                            "Only S3 to local copies are currently supported: "
                                    + sourceUri
                                    + " -> "
                                    + request.getDestination());
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
        } catch (Throwable e) {
            cancellation.close();
            ExceptionUtils.rethrowIOException(e);
        } finally {
            downloadPool.shutdownNow();
        }
    }

    /**
     * Initiates an async S3 to local file copy.
     *
     * <p>The object body is streamed to disk through a bounded {@code byte[]} buffer (see {@code
     * s3.bulk-copy.download-buffer-size}) rather than the SDK's {@code AsynchronousFileChannel}
     * based {@code toFile} transformer, which caches an unbounded per-thread temporary direct
     * buffer sized to each write and can exhaust direct memory during large restores.
     *
     * @param request the copy request containing source S3 path and destination local path
     * @param downloadPool the executor on which the blocking stream copy runs
     * @return a CompletableFuture that completes when the download finishes
     * @throws IOException if the destination directory cannot be created
     */
    private CompletableFuture<Void> copyS3ToLocal(
            PathsCopyingFileSystem.CopyRequest request,
            ExecutorService downloadPool,
            BulkCopyCancellation cancellation)
            throws IOException {

        String sourceUri = request.getSource().toUri().toString();
        String bucket = extractBucket(sourceUri);
        String key = extractKey(sourceUri);
        Path destination = new File(request.getDestination().getPath()).toPath().toAbsolutePath();

        Path parent = destination.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tempDestination = NativeS3FileIoUtils.createTemporaryDownloadFile(parent, destination);

        GetObjectRequest getObjectRequest =
                GetObjectRequest.builder().bucket(bucket).key(key).build();

        CompletableFuture<ResponseInputStream<GetObjectResponse>> responseFuture;
        try {
            responseFuture =
                    asyncClient.getObject(
                            getObjectRequest, AsyncResponseTransformer.toBlockingInputStream());
        } catch (RuntimeException | Error e) {
            IOUtils.deleteFileQuietly(tempDestination);
            throw e;
        }
        cancellation.registerFuture(responseFuture);
        CompletableFuture<Void> copyFuture = new CompletableFuture<>();
        cancellation.registerFuture(copyFuture);
        responseFuture.whenComplete(
                (responseStream, error) -> {
                    cancellation.unregisterFuture(responseFuture);
                    if (error != null) {
                        IOUtils.deleteFileQuietly(tempDestination);
                        copyFuture.completeExceptionally(error);
                        return;
                    }
                    if (responseStream == null) {
                        IOUtils.deleteFileQuietly(tempDestination);
                        copyFuture.completeExceptionally(
                                new IOException(
                                        "S3 getObject completed without a response stream"));
                        return;
                    }
                    submitDownload(
                            responseStream,
                            tempDestination,
                            destination,
                            sourceUri,
                            downloadPool,
                            cancellation,
                            copyFuture);
                });
        copyFuture.whenComplete(
                (ignored, error) -> {
                    cancellation.unregisterFuture(copyFuture);
                    if (copyFuture.isCancelled()) {
                        responseFuture.cancel(true);
                    }
                    if (error != null) {
                        IOUtils.deleteFileQuietly(tempDestination);
                    }
                });
        return copyFuture;
    }

    private void submitDownload(
            ResponseInputStream<GetObjectResponse> responseStream,
            Path tempDestination,
            Path destination,
            String sourceUri,
            ExecutorService downloadPool,
            BulkCopyCancellation cancellation,
            CompletableFuture<Void> copyFuture) {
        if (!cancellation.registerStream(responseStream)) {
            abortAndClose(responseStream);
            IOUtils.deleteFileQuietly(tempDestination);
            copyFuture.completeExceptionally(new CancellationException("Bulk copy was cancelled"));
            return;
        }
        try {
            downloadPool.execute(
                    () -> {
                        boolean success = false;
                        try {
                            NativeS3FileIoUtils.copyStream(
                                    responseStream, tempDestination, downloadBufferSize);
                            NativeS3FileIoUtils.moveFile(tempDestination, destination);
                            success = true;
                            LOG.debug("Successfully copied {} to {}", sourceUri, destination);
                            copyFuture.complete(null);
                        } catch (Throwable t) {
                            copyFuture.completeExceptionally(t);
                        } finally {
                            cancellation.unregisterStream(responseStream);
                            // Close on success (stream fully read, connection reusable); abort on
                            // failure to drop the connection immediately instead of draining a
                            // large partially-read body.
                            if (success) {
                                closeQuietly(responseStream);
                            } else {
                                abortAndClose(responseStream);
                            }
                            IOUtils.deleteFileQuietly(tempDestination);
                        }
                    });
        } catch (RejectedExecutionException e) {
            cancellation.unregisterStream(responseStream);
            abortAndClose(responseStream);
            IOUtils.deleteFileQuietly(tempDestination);
            copyFuture.completeExceptionally(e);
        }
    }

    private void waitForCopies(List<CompletableFuture<Void>> futures) throws IOException {
        try {
            // Fail fast: complete as soon as either all downloads finish successfully or the first
            // one fails, rather than waiting for every in-flight download to run to completion. The
            // outer copyFiles handler aborts the remaining streams and shuts the pool down on the
            // resulting exception.
            CompletableFuture<Void> allDone =
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            CompletableFuture<Void> firstFailure = new CompletableFuture<>();
            for (CompletableFuture<Void> future : futures) {
                future.whenComplete(
                        (ignored, error) -> {
                            if (error != null) {
                                firstFailure.completeExceptionally(error);
                            }
                        });
            }
            CompletableFuture.anyOf(allDone, firstFailure).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Bulk copy interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            ExceptionUtils.rethrowIfFatalError(cause);
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

    static boolean isSupportedS3Scheme(org.apache.flink.core.fs.Path path) {
        String scheme = path.toUri().getScheme();
        return "s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme);
    }

    static boolean isSupportedLocalScheme(org.apache.flink.core.fs.Path path) {
        String scheme = path.toUri().getScheme();
        return scheme == null || "file".equalsIgnoreCase(scheme);
    }

    private static void abortAndClose(ResponseInputStream<GetObjectResponse> stream) {
        try {
            stream.abort();
        } catch (RuntimeException e) {
            LOG.debug("Error aborting S3 response stream during bulk-copy cancellation", e);
        }
        try {
            stream.close();
        } catch (IOException e) {
            LOG.debug("Error closing S3 response stream during bulk-copy cancellation", e);
        }
    }

    private static void closeQuietly(ResponseInputStream<GetObjectResponse> stream) {
        try {
            stream.close();
        } catch (IOException e) {
            LOG.debug("Error closing S3 response stream after successful bulk-copy download", e);
        }
    }

    private static final class BulkCopyCancellation implements Closeable {
        private static final long TERMINATION_TIMEOUT_SECONDS = 5L;

        private final ExecutorService downloadPool;
        private final Set<CompletableFuture<?>> futures =
                Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
        private final Set<ResponseInputStream<GetObjectResponse>> activeStreams =
                Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private BulkCopyCancellation(ExecutorService downloadPool) {
            this.downloadPool = downloadPool;
        }

        private void registerFuture(CompletableFuture<?> future) {
            if (closed.get()) {
                future.cancel(true);
                return;
            }
            futures.add(future);
            if (closed.get() && futures.remove(future)) {
                future.cancel(true);
            }
        }

        private void unregisterFuture(CompletableFuture<?> future) {
            futures.remove(future);
        }

        private boolean registerStream(ResponseInputStream<GetObjectResponse> stream) {
            if (closed.get()) {
                return false;
            }
            activeStreams.add(stream);
            if (closed.get() && activeStreams.remove(stream)) {
                return false;
            }
            return true;
        }

        private void unregisterStream(ResponseInputStream<GetObjectResponse> stream) {
            activeStreams.remove(stream);
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            futures.forEach(future -> future.cancel(true));
            activeStreams.forEach(NativeS3BulkCopyHelper::abortAndClose);
            downloadPool.shutdownNow();
            try {
                if (!downloadPool.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "S3 bulk-copy worker pool did not terminate within {} seconds",
                            TERMINATION_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
