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
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.s3.common.utils.BackPressuringExecutor;
import org.apache.flink.fs.s3.common.utils.RefCountedFile;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * A factory for creating or recovering {@link RecoverableMultiPartUpload mulitpart uploads}.
 */
@Internal
final class S3RecoverableMultipartUploadFactory {

	private final org.apache.hadoop.fs.FileSystem fs;

	private final S3AccessHelper s3AccessHelper;

	private final FunctionWithException<File, RefCountedFile, IOException> tmpFileSupplier;

	private final int maxConcurrentUploadsPerStream;

	private final Executor executor;

	S3RecoverableMultipartUploadFactory(
			final FileSystem fs,
			final S3AccessHelper s3AccessHelper,
			final int maxConcurrentUploadsPerStream,
			final Executor executor,
			final FunctionWithException<File, RefCountedFile, IOException> tmpFileSupplier) {

		this.fs = Preconditions.checkNotNull(fs);
		this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
		this.executor = executor;
		this.s3AccessHelper = s3AccessHelper;
		this.tmpFileSupplier = tmpFileSupplier;
	}

	RecoverableMultiPartUpload getNewRecoverableUpload(Path path) throws IOException {

		return RecoverableMultiPartUploadImpl.newUpload(
				s3AccessHelper,
				limitedExecutor(),
				pathToObjectName(path));
	}

	RecoverableMultiPartUpload recoverRecoverableUpload(S3Recoverable recoverable) throws IOException {
		final Optional<File> incompletePart = recoverInProgressPart(recoverable);

		return RecoverableMultiPartUploadImpl.recoverUpload(
				s3AccessHelper,
				limitedExecutor(),
				recoverable.uploadId(),
				recoverable.getObjectName(),
				recoverable.parts(),
				recoverable.numBytesInParts(),
				incompletePart);
	}

	private Optional<File> recoverInProgressPart(S3Recoverable recoverable) throws IOException {

		final String objectKey = recoverable.incompleteObjectName();
		if (objectKey == null) {
			return Optional.empty();
		}

		// download the file (simple way)
		final RefCountedFile refCountedFile = tmpFileSupplier.apply(null);
		final File file = refCountedFile.getFile();
		final long numBytes = s3AccessHelper.getObject(objectKey, file);

		if (numBytes != recoverable.incompleteObjectLength()) {
			throw new IOException(String.format("Error recovering writer: " +
							"Downloading the last data chunk file gives incorrect length." +
							"File length is %d bytes, RecoveryData indicates %d bytes",
					numBytes, recoverable.incompleteObjectLength()));
		}

		return Optional.of(file);
	}

	private String pathToObjectName(final Path path) {
		org.apache.hadoop.fs.Path hadoopPath = HadoopFileSystem.toHadoopPath(path);
		if (!hadoopPath.isAbsolute()) {
			hadoopPath = new org.apache.hadoop.fs.Path(fs.getWorkingDirectory(), hadoopPath);
		}

		return hadoopPath.toUri().getScheme() != null && hadoopPath.toUri().getPath().isEmpty()
				? ""
				: hadoopPath.toUri().getPath().substring(1);
	}

	private Executor limitedExecutor() {
		return maxConcurrentUploadsPerStream <= 0 ?
				executor :
				new BackPressuringExecutor(executor, maxConcurrentUploadsPerStream);
	}
}
