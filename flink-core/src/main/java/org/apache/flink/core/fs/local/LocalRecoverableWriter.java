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

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RecoverableWriter} for the {@link LocalFileSystem}.
 */
@Internal
public class LocalRecoverableWriter implements RecoverableWriter {

	private final LocalFileSystem fs;

	public LocalRecoverableWriter(LocalFileSystem fs) {
		this.fs = checkNotNull(fs);
	}

	@Override
	public RecoverableFsDataOutputStream open(Path filePath) throws IOException {
		final File targetFile = fs.pathToFile(filePath);
		final File tempFile = generateStagingTempFilePath(targetFile);

		// try to create the parent
		final File parent = tempFile.getParentFile();
		if (parent != null && !parent.mkdirs() && !parent.exists()) {
			throw new IOException("Failed to create the parent directory: " + parent);
		}

		return new LocalRecoverableFsDataOutputStream(targetFile, tempFile);
	}

	@Override
	public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
		if (recoverable instanceof LocalRecoverable) {
			return new LocalRecoverableFsDataOutputStream((LocalRecoverable) recoverable);
		}
		else {
			throw new IllegalArgumentException(
					"LocalFileSystem cannot recover recoverable for other file system: " + recoverable);
		}
	}

	@Override
	public boolean requiresCleanupOfRecoverableState() {
		return false;
	}

	@Override
	public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Committer recoverForCommit(CommitRecoverable recoverable) throws IOException {
		if (recoverable instanceof LocalRecoverable) {
			return new LocalRecoverableFsDataOutputStream.LocalCommitter((LocalRecoverable) recoverable);
		}
		else {
			throw new IllegalArgumentException(
					"LocalFileSystem cannot recover recoverable for other file system: " + recoverable);
		}
	}

	@Override
	public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
		@SuppressWarnings("unchecked")
		SimpleVersionedSerializer<CommitRecoverable> typedSerializer = (SimpleVersionedSerializer<CommitRecoverable>)
				(SimpleVersionedSerializer<?>) LocalRecoverableSerializer.INSTANCE;

		return typedSerializer;
	}

	@Override
	public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
		@SuppressWarnings("unchecked")
		SimpleVersionedSerializer<ResumeRecoverable> typedSerializer = (SimpleVersionedSerializer<ResumeRecoverable>)
				(SimpleVersionedSerializer<?>) LocalRecoverableSerializer.INSTANCE;

		return typedSerializer;
	}

	@Override
	public boolean supportsResume() {
		return true;
	}

	@VisibleForTesting
	static File generateStagingTempFilePath(File targetFile) {
		checkArgument(targetFile.isAbsolute(), "targetFile must be absolute");
		checkArgument(!targetFile.isDirectory(), "targetFile must not be a directory");

		final File parent = targetFile.getParentFile();
		final String name = targetFile.getName();

		checkArgument(parent != null, "targetFile must not be the root directory");

		while (true) {
			File candidate = new File(parent, "." + name + ".inprogress." + UUID.randomUUID().toString());
			if (!candidate.exists()) {
				return candidate;
			}
		}
	}
}
