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

package org.apache.flink.fs.s3native.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.s3native.writer.NativeS3Recoverable.PartETag;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A recoverable output stream writes data to S3 using multipart uploads.
 *
 * <p>This class is NOT thread-safe. All write operations ({@link #write}, {@link #flush}, {@link
 * #persist}, {@link #closeForCommit}) must be called from a single thread (the Flink operator
 * thread). This is consistent with Flink's {@link RecoverableFsDataOutputStream} contract where
 * output streams are confined to a single task thread.
 *
 * <p>The {@link #close()} method may be called concurrently (e.g., during task cancellation). A
 * {@link ReentrantLock} guards the critical sections in {@link #close()}, {@link
 * #closeForCommit()}, {@link #persist()} and the part buffer rotation in {@link #write} to ensure
 * safe cleanup of local resources without corrupting S3 state.
 *
 * <p>{@link #close()} waits for the part uploads that are still in flight. Otherwise such an upload
 * could finish after the stream was recovered from a checkpoint, replacing a part with the same
 * number that the recovered stream uploaded and invalidating the ETag recorded for it. Parts whose
 * upload has not started, e.g. because they are queued behind the parts of other streams, are not
 * uploaded anymore, so it does not wait for them.
 *
 * <p>This stream never aborts its multipart upload. The upload may be referenced by a {@link
 * NativeS3Recoverable} in a retained checkpoint or savepoint, and the stream cannot know whether
 * that is the case. Aborting it would make the referencing snapshot unrecoverable. Abandoned
 * uploads are expected to be removed by an S3 lifecycle rule for incomplete multipart uploads, as
 * with the Hadoop-based S3 file system.
 */
@NotThreadSafe
class NativeS3RecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(NativeS3RecoverableFsDataOutputStream.class);

    private static final int BUFFER_SIZE = 64 * 1024;
    private final ReentrantLock lock = new ReentrantLock();

    private final NativeS3ObjectOperations s3AccessHelper;
    private final Executor uploadExecutor;
    private final String key;
    private final String uploadId;
    private final String localTmpDir;
    private final long minPartSize;

    private final List<PartETag> completedParts;

    /** Part uploads that have not been added to {@link #completedParts} yet, in part order. */
    private final Deque<PendingPartUpload> pendingUploads = new ConcurrentLinkedDeque<>();

    private long numBytesInParts;

    /**
     * The first failure that broke the stream, usually a failed part upload. The upload threads set
     * it as well, so that the stream stops accepting data as soon as any part fails, not only once
     * the parts before it are done.
     */
    private final AtomicReference<IOException> streamFailure = new AtomicReference<>();

    private File currentTempFile;
    private FileOutputStream currentFileStream;
    private BufferedOutputStream currentOutputStream;
    private long currentPartSize;
    private int nextPartNumber;

    private volatile boolean closed;

    public NativeS3RecoverableFsDataOutputStream(
            NativeS3ObjectOperations s3AccessHelper,
            Executor uploadExecutor,
            String key,
            String uploadId,
            String localTmpDir,
            long minPartSize)
            throws IOException {
        this(
                s3AccessHelper,
                uploadExecutor,
                key,
                uploadId,
                localTmpDir,
                minPartSize,
                new ArrayList<>(),
                0L,
                null);
    }

    public NativeS3RecoverableFsDataOutputStream(
            NativeS3ObjectOperations s3AccessHelper,
            Executor uploadExecutor,
            String key,
            String uploadId,
            String localTmpDir,
            long minPartSize,
            List<PartETag> existingParts,
            long numBytesInParts,
            File incompleteTailFile)
            throws IOException {
        this.s3AccessHelper = s3AccessHelper;
        this.uploadExecutor = uploadExecutor;
        this.key = key;
        this.uploadId = uploadId;
        this.localTmpDir = localTmpDir;
        this.minPartSize = minPartSize;
        this.completedParts = new ArrayList<>(existingParts);
        this.numBytesInParts = numBytesInParts;
        this.nextPartNumber = existingParts.size() + 1;
        this.currentPartSize = 0;
        this.closed = false;

        if (incompleteTailFile != null) {
            resumeFromIncompleteTail(incompleteTailFile);
        } else {
            createNewTempFile();
        }
    }

    private void resumeFromIncompleteTail(File tailFile) throws IOException {
        if (!tailFile.exists()) {
            throw new IOException("Incomplete-tail file does not exist: " + tailFile);
        }
        currentTempFile = tailFile;
        currentPartSize = tailFile.length();
        // Append mode so subsequent writes land after the recovered bytes.
        currentFileStream = new FileOutputStream(currentTempFile, true);
        currentOutputStream = new BufferedOutputStream(currentFileStream, BUFFER_SIZE);
    }

    @VisibleForTesting
    protected void createNewTempFile() throws IOException {
        File tmpDir = new File(localTmpDir);
        Files.createDirectories(tmpDir.toPath());

        currentTempFile = new File(tmpDir, "s3-part-" + UUID.randomUUID());
        currentFileStream = new FileOutputStream(currentTempFile);
        currentOutputStream = new BufferedOutputStream(currentFileStream, BUFFER_SIZE);
        currentPartSize = 0;
    }

    @Override
    public long getPos() throws IOException {
        return numBytesInParts + currentPartSize;
    }

    @Override
    public void write(int b) throws IOException {
        checkUsable();

        currentOutputStream.write(b);
        currentPartSize++;

        if (currentPartSize >= minPartSize) {
            rotatePart();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkUsable();
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        currentOutputStream.write(b, off, len);
        currentPartSize += len;

        if (currentPartSize >= minPartSize) {
            rotatePart();
        }
    }

    @Override
    public void flush() throws IOException {
        checkUsable();
        currentOutputStream.flush();
    }

    @Override
    public void sync() throws IOException {
        checkUsable();
        persist();
    }

    private void checkUsable() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        final IOException failure = streamFailure.get();
        if (failure != null) {
            // Rethrown as it is, so that the task handles a fatal error or an OutOfMemoryError
            // of an upload thread like one of its own thread, e.g. by terminating the JVM.
            ExceptionUtils.rethrowIfFatalErrorOrOOM(failure.getCause());
            throw new IOException(
                    "This stream failed. It cannot be used anymore and "
                            + "must be recovered from the last checkpoint.",
                    failure);
        }
    }

    private void uploadCurrentPart() throws IOException {
        final PendingPartUpload upload;
        lock();
        try {
            // close() may have run concurrently and released the buffer.
            checkUsable();
            currentOutputStream.close();

            upload = new PendingPartUpload(nextPartNumber, currentTempFile, currentPartSize);
            pendingUploads.addLast(upload);
            nextPartNumber++;
            numBytesInParts += currentPartSize;
            currentOutputStream = null;
            currentFileStream = null;
            currentTempFile = null;
            currentPartSize = 0;
        } finally {
            unlock();
        }

        // Blocks while the maximum number of uploads is in flight. Called from write(), this does
        // not hold the lock, so close() is not blocked by back-pressure.
        try {
            uploadExecutor.execute(() -> uploadPart(upload));
        } catch (Throwable t) {
            // E.g. a RejectedExecutionException once the file system is closed, or an
            // OutOfMemoryError if no thread can be created.
            final IOException failure = failUpload(upload, t);
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            throw failure;
        }
        completeFinishedUploads();
    }

    private void rotatePart() throws IOException {
        uploadCurrentPart();
        try {
            startNewPart();
        } catch (Throwable t) {
            // The part was handed over, so the stream has no buffer anymore. Fail it, so that
            // later calls throw a clear exception instead of a NullPointerException.
            streamFailure.compareAndSet(
                    null,
                    new IOException(
                            String.format(
                                    "Failed to start part %d of multipart upload %s for key %s",
                                    nextPartNumber, uploadId, key),
                            t));
            throw t;
        }
    }

    private void startNewPart() throws IOException {
        lock();
        try {
            // close() may have run while uploadCurrentPart() waited for a free upload slot.
            checkUsable();
            createNewTempFile();
        } finally {
            unlock();
        }
    }

    private void uploadPart(PendingPartUpload upload) {
        // Once the stream is closed, close() takes over the parts that have not started.
        if (closed || !upload.claim()) {
            return;
        }
        try {
            final NativeS3ObjectOperations.UploadPartResult result =
                    s3AccessHelper.uploadPart(
                            key, uploadId, upload.partNumber, upload.file, upload.length);
            Files.deleteIfExists(upload.file.toPath());
            upload.future.complete(new PartETag(result.getPartNumber(), result.getETag()));
        } catch (Throwable t) {
            failUpload(upload, t);
        }
    }

    /**
     * Marks the stream as failed and completes the upload exceptionally. The failure is recorded
     * first, so that whoever sees the failed upload also sees the stream as failed. The upload is
     * completed even if recording the failure fails, e.g. with an {@link OutOfMemoryError}, because
     * {@link #close()} waits for it.
     */
    private IOException failUpload(PendingPartUpload upload, Throwable cause) {
        try {
            return recordUploadFailure(upload, cause);
        } finally {
            upload.future.completeExceptionally(cause);
        }
    }

    private IOException recordUploadFailure(PendingPartUpload upload, Throwable cause) {
        final IOException failure =
                new IOException(
                        String.format(
                                "Failed to upload part %d of multipart upload %s for key %s",
                                upload.partNumber, uploadId, key),
                        cause);
        streamFailure.compareAndSet(null, failure);
        return failure;
    }

    private void completeFinishedUploads() throws IOException {
        PendingPartUpload upload;
        while ((upload = pendingUploads.peekFirst()) != null && upload.future.isDone()) {
            completePendingUpload(upload);
        }
    }

    private void awaitPendingUploads() throws IOException {
        PendingPartUpload upload;
        while ((upload = pendingUploads.peekFirst()) != null) {
            completePendingUpload(upload);
        }
    }

    private void completePendingUpload(PendingPartUpload upload) throws IOException {
        final PartETag partETag;
        try {
            partETag = upload.future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while waiting for the upload of part " + upload.partNumber, e);
        } catch (ExecutionException e) {
            final IOException failure = recordUploadFailure(upload, e.getCause());
            // See checkUsable().
            ExceptionUtils.rethrowIfFatalErrorOrOOM(e.getCause());
            throw failure;
        }
        completedParts.add(partETag);
        pendingUploads.removeFirst();
    }

    @Override
    public Committer closeForCommit() throws IOException {
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is already closed");
            }

            final NativeS3Recoverable recoverable;
            try {
                checkUsable();
                currentOutputStream.close();

                if (currentPartSize > 0) {
                    uploadCurrentPart();
                } else {
                    Files.deleteIfExists(currentTempFile.toPath());
                }
                awaitPendingUploads();

                recoverable =
                        new NativeS3Recoverable(
                                key, uploadId, new ArrayList<>(completedParts), numBytesInParts);
            } catch (IOException e) {
                // Only local resources are released. The upload is deliberately left open: a
                // previous persist() may have handed it out in a recoverable that a completed
                // checkpoint references, and aborting it would break recovery from that
                // checkpoint. See the class-level Javadoc.
                closed = true;
                LOG.warn(
                        "Failed to close multipart upload for commit (key={}, uploadId={}). The "
                                + "upload is left open because a checkpoint may still reference "
                                + "it; an S3 lifecycle rule is expected to remove it if unused.",
                        key,
                        uploadId,
                        e);
                try {
                    releaseLocalResources();
                } catch (IOException cleanup) {
                    e.addSuppressed(cleanup);
                }
                throw e;
            }

            closed = true;
            return new NativeS3Committer(s3AccessHelper, recoverable);
        } finally {
            unlock();
        }
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        lock();
        try {
            flush();
            awaitPendingUploads();

            String incompletePartKey = null;
            long incompletePartLength = 0;

            if (currentPartSize > 0) {
                currentOutputStream.flush();
                // Keep the side object in the target's directory, so that anything scoped to the
                // target's key prefix (for example IAM or session policies) covers it as well.
                final String parent = key.substring(0, key.lastIndexOf('/') + 1);
                incompletePartKey = parent + ".incomplete/" + uploadId + "/" + UUID.randomUUID();
                s3AccessHelper.putObject(incompletePartKey, currentTempFile);
                incompletePartLength = currentPartSize;
            }

            return new NativeS3Recoverable(
                    key,
                    uploadId,
                    new ArrayList<>(completedParts),
                    numBytesInParts,
                    incompletePartKey,
                    incompletePartLength);
        } finally {
            unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // Not interruptibly: on cancellation the thread is usually interrupted, and close() must
        // still release the local resources and wait for the in-flight part uploads.
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                releaseLocalResources();
            }
        } finally {
            unlock();
        }
    }

    /**
     * Closes the local buffer, waits for the part uploads that are still in flight, keeps the ones
     * that have not started from starting and deletes only the local temp files. See the
     * class-level Javadoc for why it waits for the uploads.
     */
    private void releaseLocalResources() throws IOException {
        IOException collected = null;
        if (currentOutputStream != null) {
            try {
                currentOutputStream.close();
            } catch (IOException e) {
                collected = ExceptionUtils.firstOrSuppressed(e, collected);
            }
        }
        if (currentTempFile != null) {
            try {
                deleteTempFile(currentTempFile);
            } catch (IOException e) {
                collected = ExceptionUtils.firstOrSuppressed(e, collected);
            }
        }
        for (PendingPartUpload upload : pendingUploads) {
            if (upload.claim()) {
                // Not started, e.g. because it is queued behind the parts of other streams. It
                // cannot start anymore, so there is nothing to wait for.
                upload.future.completeExceptionally(
                        new IOException(
                                "The stream was closed before the upload of part "
                                        + upload.partNumber
                                        + " started."));
            } else {
                // join() is not interruptible, and handle() turns a failed upload into a result.
                final Throwable uploadError =
                        upload.future.handle((ignored, error) -> error).join();
                if (uploadError != null) {
                    // Logged, because close() may run after an interrupt that hides the S3 error.
                    LOG.warn(
                            "Upload of part {} of multipart upload {} for key {} failed.",
                            upload.partNumber,
                            uploadId,
                            key,
                            uploadError);
                }
            }
            try {
                deleteTempFile(upload.file);
            } catch (IOException e) {
                collected = ExceptionUtils.firstOrSuppressed(e, collected);
            }
        }
        if (collected != null) {
            throw collected;
        }
    }

    @VisibleForTesting
    protected void deleteTempFile(File file) throws IOException {
        Files.deleteIfExists(file.toPath());
    }

    private void lock() throws IOException {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while acquiring lock", e);
        }
    }

    private void unlock() {
        lock.unlock();
    }

    /** A part handed over to the upload executor. */
    private static final class PendingPartUpload {
        private final int partNumber;
        private final File file;
        private final long length;
        private final CompletableFuture<PartETag> future = new CompletableFuture<>();

        /** Set by the upload thread when it starts the upload, or by close() before that. */
        private final AtomicBoolean claimed = new AtomicBoolean();

        private PendingPartUpload(int partNumber, File file, long length) {
            this.partNumber = partNumber;
            this.file = file;
            this.length = length;
        }

        /** Returns whether the caller is the only one that handles the upload. */
        private boolean claim() {
            return claimed.compareAndSet(false, true);
        }
    }
}
