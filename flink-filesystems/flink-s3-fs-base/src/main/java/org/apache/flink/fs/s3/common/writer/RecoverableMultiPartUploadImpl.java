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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.fs.s3.common.utils.RefCountedFSOutputStream;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartResult;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An uploader for parts of a multipart upload. The uploader can snapshot its state to
 * be recovered after a failure.
 *
 * <p><b>Note:</b> This class is NOT thread safe and relies on external synchronization.
 *
 * <p><b>Note:</b> If any of the methods to add parts throws an exception, this class may be
 * in an inconsistent state (bookkeeping wise) and should be discarded and recovered.
 */
@Internal
@NotThreadSafe
final class RecoverableMultiPartUploadImpl implements RecoverableMultiPartUpload {

	private final S3AccessHelper s3AccessHelper;

	private final Executor uploadThreadPool;

	private final Deque<CompletableFuture<PartETag>> uploadsInProgress;

	private final String namePrefixForTempObjects;

	private final MultiPartUploadInfo currentUploadInfo;

	// ------------------------------------------------------------------------

	private RecoverableMultiPartUploadImpl(
			S3AccessHelper s3AccessHelper,
			Executor uploadThreadPool,
			String uploadId,
			String objectName,
			List<PartETag> partsSoFar,
			long numBytes,
			Optional<File> incompletePart
	) {
		checkArgument(numBytes >= 0L);

		this.s3AccessHelper = checkNotNull(s3AccessHelper);
		this.uploadThreadPool = checkNotNull(uploadThreadPool);
		this.currentUploadInfo = new MultiPartUploadInfo(objectName, uploadId, partsSoFar, numBytes, incompletePart);
		this.namePrefixForTempObjects = createIncompletePartObjectNamePrefix(objectName);
		this.uploadsInProgress = new ArrayDeque<>();
	}

	/**
	 * Adds a part to the uploads without any size limitations.
	 *
	 * <p>This method is non-blocking and does not wait for the part upload to complete.
	 *
	 * @param file The file with the part data.
	 *
	 * @throws IOException If this method throws an exception, the RecoverableS3MultiPartUpload
	 *                     should not be used any more, but recovered instead.
	 */
	@Override
	public void uploadPart(RefCountedFSOutputStream file) throws IOException {
		// this is to guarantee that nobody is
		// writing to the file we are uploading.
		checkState(file.isClosed());

		final CompletableFuture<PartETag> future = new CompletableFuture<>();
		uploadsInProgress.add(future);

		final long partLength = file.getPos();
		currentUploadInfo.registerNewPart(partLength);

		file.retain(); // keep the file while the async upload still runs
		uploadThreadPool.execute(new UploadTask(s3AccessHelper, currentUploadInfo, file, future));
	}

	@Override
	public Optional<File> getIncompletePart() {
		return currentUploadInfo.getIncompletePart();
	}

	@Override
	public S3Committer snapshotAndGetCommitter() throws IOException {
		final S3Recoverable snapshot = snapshotAndGetRecoverable(null);

		return new S3Committer(
				s3AccessHelper,
				snapshot.getObjectName(),
				snapshot.uploadId(),
				snapshot.parts(),
				snapshot.numBytesInParts());
	}

	/**
	 * Creates a snapshot of this MultiPartUpload, from which the upload can be resumed.
	 *
	 * <p>Data buffered locally which is less than
	 * {@link org.apache.flink.fs.s3.common.FlinkS3FileSystem#S3_MULTIPART_MIN_PART_SIZE S3_MULTIPART_MIN_PART_SIZE},
	 * and cannot be uploaded as part of the MPU and set to S3 as independent objects.
	 *
	 * <p>This implementation currently blocks until all part uploads are complete and returns
	 * a completed future.
	 */
	@Override
	public S3Recoverable snapshotAndGetRecoverable(@Nullable final RefCountedFSOutputStream incompletePartFile) throws IOException {

		final String incompletePartObjectName = safelyUploadSmallPart(incompletePartFile);

		// make sure all other uploads are complete
		// this currently makes the method blocking,
		// to be made non-blocking in the future
		awaitPendingPartsUpload();

		final String objectName = currentUploadInfo.getObjectName();
		final String uploadId = currentUploadInfo.getUploadId();
		final List<PartETag> completedParts = currentUploadInfo.getCopyOfEtagsOfCompleteParts();
		final long sizeInBytes = currentUploadInfo.getExpectedSizeInBytes();

		if (incompletePartObjectName == null) {
			return new S3Recoverable(objectName, uploadId, completedParts, sizeInBytes);
		} else {
			return new S3Recoverable(objectName, uploadId, completedParts, sizeInBytes, incompletePartObjectName, incompletePartFile.getPos());
		}
	}

	@Nullable
	private String safelyUploadSmallPart(@Nullable RefCountedFSOutputStream file) throws IOException {

		if (file == null || file.getPos() == 0L) {
			return null;
		}

		// first, upload the trailing data file. during that time, other in-progress uploads may complete.
		final String incompletePartObjectName = createIncompletePartObjectName();
		file.retain();
		try {
			s3AccessHelper.putObject(incompletePartObjectName, file.getInputFile());
		}
		finally {
			file.release();
		}
		return incompletePartObjectName;
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	@VisibleForTesting
	static String createIncompletePartObjectNamePrefix(String objectName) {
		checkNotNull(objectName);

		final int lastSlash = objectName.lastIndexOf('/');
		final String parent;
		final String child;

		if (lastSlash == -1) {
			parent = "";
			child = objectName;
		} else {
			parent = objectName.substring(0, lastSlash + 1);
			child = objectName.substring(lastSlash + 1);
		}
		return parent + (child.isEmpty() ? "" : '_') + child + "_tmp_";
	}

	private String createIncompletePartObjectName() {
		return namePrefixForTempObjects + UUID.randomUUID().toString();
	}

	private void awaitPendingPartsUpload() throws IOException {
		checkState(currentUploadInfo.getRemainingParts() == uploadsInProgress.size());

		while (currentUploadInfo.getRemainingParts() > 0) {
			CompletableFuture<PartETag> next = uploadsInProgress.peekFirst();
			PartETag nextPart = awaitPendingPartUploadToComplete(next);
			currentUploadInfo.registerCompletePart(nextPart);
			uploadsInProgress.removeFirst();
		}
	}

	private PartETag awaitPendingPartUploadToComplete(CompletableFuture<PartETag> upload) throws IOException {
		final PartETag completedUploadEtag;
		try {
			completedUploadEtag = upload.get();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("Interrupted while waiting for part uploads to complete");
		}
		catch (ExecutionException e) {
			throw new IOException("Uploading parts failed", e.getCause());
		}
		return completedUploadEtag;
	}

	// ------------------------------------------------------------------------
	//  factory methods
	// ------------------------------------------------------------------------

	public static RecoverableMultiPartUploadImpl newUpload(
			final S3AccessHelper s3AccessHelper,
			final Executor uploadThreadPool,
			final String objectName) throws IOException {

		final String multiPartUploadId = s3AccessHelper.startMultiPartUpload(objectName);

		return new RecoverableMultiPartUploadImpl(
				s3AccessHelper,
				uploadThreadPool,
				multiPartUploadId,
				objectName,
				new ArrayList<>(),
				0L,
				Optional.empty());
	}

	public static RecoverableMultiPartUploadImpl recoverUpload(
			final S3AccessHelper s3AccessHelper,
			final Executor uploadThreadPool,
			final String multipartUploadId,
			final String objectName,
			final List<PartETag> partsSoFar,
			final long numBytesSoFar,
			final Optional<File> incompletePart) {

		return new RecoverableMultiPartUploadImpl(
				s3AccessHelper,
				uploadThreadPool,
				multipartUploadId,
				objectName,
				new ArrayList<>(partsSoFar),
				numBytesSoFar,
				incompletePart);

	}

	// ------------------------------------------------------------------------
	//  factory methods
	// ------------------------------------------------------------------------

	private static class UploadTask implements Runnable {

		private final S3AccessHelper s3AccessHelper;

		private final String objectName;

		private final String uploadId;

		private final int partNumber;

		private final RefCountedFSOutputStream file;

		private final CompletableFuture<PartETag> future;

		UploadTask(
				final S3AccessHelper s3AccessHelper,
				final MultiPartUploadInfo currentUpload,
				final RefCountedFSOutputStream file,
				final CompletableFuture<PartETag> future) {

			checkNotNull(currentUpload);

			this.objectName = currentUpload.getObjectName();
			this.uploadId = currentUpload.getUploadId();
			this.partNumber = currentUpload.getNumberOfRegisteredParts();

			// these are limits put by Amazon
			checkArgument(partNumber >= 1  && partNumber <= 10_000);

			this.s3AccessHelper = checkNotNull(s3AccessHelper);
			this.file = checkNotNull(file);
			this.future = checkNotNull(future);
		}

		@Override
		public void run() {
			try {
				final UploadPartResult result = s3AccessHelper.uploadPart(objectName, uploadId, partNumber, file.getInputFile(), file.getPos());
				future.complete(new PartETag(result.getPartNumber(), result.getETag()));
				file.release();
			}
			catch (Throwable t) {
				future.completeExceptionally(t);
			}
		}
	}
}
