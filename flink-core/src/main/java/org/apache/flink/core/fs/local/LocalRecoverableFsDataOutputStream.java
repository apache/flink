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
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RecoverableFsDataOutputStream} for the {@link LocalFileSystem}.
 */
@Internal
class LocalRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

	private final File targetFile;

	private final File tempFile;

	private final FileChannel fileChannel;

	private final OutputStream fos;

	LocalRecoverableFsDataOutputStream(File targetFile, File tempFile) throws IOException {
		this.targetFile = checkNotNull(targetFile);
		this.tempFile = checkNotNull(tempFile);

		this.fileChannel = FileChannel.open(tempFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
		this.fos = Channels.newOutputStream(fileChannel);
	}

	LocalRecoverableFsDataOutputStream(LocalRecoverable resumable) throws IOException {
		this.targetFile = checkNotNull(resumable.targetFile());
		this.tempFile = checkNotNull(resumable.tempFile());

		if (!tempFile.exists()) {
			throw new FileNotFoundException("File Not Found: " + tempFile.getAbsolutePath());
		}

		this.fileChannel = FileChannel.open(tempFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
		if (this.fileChannel.position() < resumable.offset()) {
			throw new IOException("Missing data in tmp file: " + tempFile.getAbsolutePath());
		}
		this.fileChannel.truncate(resumable.offset());
		this.fos = Channels.newOutputStream(fileChannel);
	}

	@Override
	public void write(int b) throws IOException {
		fos.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		fos.write(b, off, len);
	}

	@Override
	public void flush() throws IOException {
		fos.flush();
	}

	@Override
	public void sync() throws IOException {
		fileChannel.force(true);
	}

	@Override
	public long getPos() throws IOException {
		return fileChannel.position();
	}

	@Override
	public ResumeRecoverable persist() throws IOException {
		// we call both flush and sync in order to ensure persistence on mounted
		// file systems, like NFS, EBS, EFS, ...
		flush();
		sync();

		return new LocalRecoverable(targetFile, tempFile, getPos());
	}

	@Override
	public Committer closeForCommit() throws IOException {
		final long pos = getPos();
		close();
		return new LocalCommitter(new LocalRecoverable(targetFile, tempFile, pos));
	}

	@Override
	public void close() throws IOException {
		fos.close();
	}

	// ------------------------------------------------------------------------

	static class LocalCommitter implements Committer {

		private final LocalRecoverable recoverable;

		LocalCommitter(LocalRecoverable recoverable) {
			this.recoverable = checkNotNull(recoverable);
		}

		@Override
		public void commit() throws IOException {
			final File src = recoverable.tempFile();
			final File dest = recoverable.targetFile();

			// sanity check
			if (src.length() != recoverable.offset()) {
				// something was done to this file since the committer was created.
				// this is not the "clean" case
				throw new IOException("Cannot clean commit: File has trailing junk data.");
			}

			// rather than fall into default recovery, handle errors explicitly
			// in order to improve error messages
			try {
				Files.move(src.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
			}
			catch (UnsupportedOperationException | AtomicMoveNotSupportedException e) {
				if (!src.renameTo(dest)) {
					throw new IOException("Committing file failed, could not rename " + src + " -> " + dest);
				}
			}
			catch (FileAlreadyExistsException e) {
				throw new IOException("Committing file failed. Target file already exists: " + dest);
			}
		}

		@Override
		public void commitAfterRecovery() throws IOException {
			final File src = recoverable.tempFile();
			final File dest = recoverable.targetFile();
			final long expectedLength = recoverable.offset();

			if (src.exists()) {
				if (src.length() > expectedLength) {
					// can happen if we co from persist to recovering for commit directly
					// truncate the trailing junk away
					try (FileOutputStream fos = new FileOutputStream(src, true)) {
						fos.getChannel().truncate(expectedLength);
					}
				} else if (src.length() < expectedLength) {
					throw new IOException("Missing data in tmp file: " + src);
				}

				// source still exists, so no renaming happened yet. do it!
				Files.move(src.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
			}
			else if (!dest.exists()) {
				// neither exists - that can be a sign of
				//   - (1) a serious problem (file system loss of data)
				//   - (2) a recovery of a savepoint that is some time old and the users
				//         removed the files in the meantime.

				// TODO how to handle this?
				// We probably need an option for users whether this should log,
				// or result in an exception or unrecoverable exception
			}
		}

		@Override
		public CommitRecoverable getRecoverable() {
			return recoverable;
		}
	}
}
