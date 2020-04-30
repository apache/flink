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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.s3.common.utils.RefCountedFile;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.fs.s3.common.FlinkS3FileSystem.S3_MULTIPART_MIN_PART_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link RecoverableWriter} against S3.
 *
 * <p>This implementation makes heavy use of MultiPart Uploads in S3 to persist
 * intermediate data as soon as possible.
 *
 * <p>This class partially reuses utility classes and implementations from the Hadoop
 * project, specifically around configuring S3 requests and handling retries.
 */
@PublicEvolving
public class S3RecoverableWriter implements RecoverableWriter {

	private final FunctionWithException<File, RefCountedFile, IOException> tempFileCreator;

	private final long userDefinedMinPartSize;

	private final S3AccessHelper s3AccessHelper;

	private final S3RecoverableMultipartUploadFactory uploadFactory;

	@VisibleForTesting
	S3RecoverableWriter(
			final S3AccessHelper s3AccessHelper,
			final S3RecoverableMultipartUploadFactory uploadFactory,
			final FunctionWithException<File, RefCountedFile, IOException> tempFileCreator,
			final long userDefinedMinPartSize) {

		this.s3AccessHelper = checkNotNull(s3AccessHelper);
		this.uploadFactory = checkNotNull(uploadFactory);
		this.tempFileCreator = checkNotNull(tempFileCreator);
		this.userDefinedMinPartSize = userDefinedMinPartSize;
	}

	@Override
	public RecoverableFsDataOutputStream open(Path path) throws IOException {
		final RecoverableMultiPartUpload upload = uploadFactory.getNewRecoverableUpload(path);

		return S3RecoverableFsDataOutputStream.newStream(
				upload,
				tempFileCreator,
				userDefinedMinPartSize);
	}

	@Override
	public Committer recoverForCommit(CommitRecoverable recoverable) throws IOException {
		final S3Recoverable s3recoverable = castToS3Recoverable(recoverable);
		final S3RecoverableFsDataOutputStream recovered = recover(s3recoverable);
		return recovered.closeForCommit();
	}

	@Override
	public S3RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
		final S3Recoverable s3recoverable = castToS3Recoverable(recoverable);

		final RecoverableMultiPartUpload upload = uploadFactory.recoverRecoverableUpload(s3recoverable);

		return S3RecoverableFsDataOutputStream.recoverStream(
				upload,
				tempFileCreator,
				userDefinedMinPartSize,
				s3recoverable.numBytesInParts());
	}

	@Override
	public boolean requiresCleanupOfRecoverableState() {
		return true;
	}

	@Override
	public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
		final S3Recoverable s3recoverable = castToS3Recoverable(resumable);
		final String smallPartObjectToDelete = s3recoverable.incompleteObjectName();
		return smallPartObjectToDelete != null && s3AccessHelper.deleteObject(smallPartObjectToDelete);
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
		return (SimpleVersionedSerializer) S3RecoverableSerializer.INSTANCE;
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
		return (SimpleVersionedSerializer) S3RecoverableSerializer.INSTANCE;
	}

	@Override
	public boolean supportsResume() {
		return true;
	}

	// --------------------------- Utils ---------------------------

	private static S3Recoverable castToS3Recoverable(CommitRecoverable recoverable) {
		if (recoverable instanceof S3Recoverable) {
			return (S3Recoverable) recoverable;
		}
		throw new IllegalArgumentException(
				"S3 File System cannot recover recoverable for other file system: " + recoverable);
	}

	// --------------------------- Static Constructor ---------------------------

	public static S3RecoverableWriter writer(
			final FileSystem fs,
			final FunctionWithException<File, RefCountedFile, IOException> tempFileCreator,
			final S3AccessHelper s3AccessHelper,
			final Executor uploadThreadPool,
			final long userDefinedMinPartSize,
			final int maxConcurrentUploadsPerStream) {

		checkArgument(userDefinedMinPartSize >= S3_MULTIPART_MIN_PART_SIZE);

		final S3RecoverableMultipartUploadFactory uploadFactory =
				new S3RecoverableMultipartUploadFactory(
						fs,
						s3AccessHelper,
						maxConcurrentUploadsPerStream,
						uploadThreadPool,
						tempFileCreator);

		return new S3RecoverableWriter(s3AccessHelper, uploadFactory, tempFileCreator, userDefinedMinPartSize);
	}
}
