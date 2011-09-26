/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;

/**
 * A writable spilling file is a temporary file that is created to store incoming data for
 * {@link AbstractByteBufferedInputChannel} objects. For performance reasons one writable spilling file may contain data
 * from multiple input channels that are associated with the same input gate.
 * <p>
 * This class is not thread-safe. Access from different threads must be protected by an external monitor.
 * 
 * @author warneke
 */
public final class WritableSpillingFile implements Closeable {

	/**
	 * The maximum period of time that is waited until the file is closed after the last write request after a read
	 * request has been issued (given in milliseconds).
	 */
	public static final long MAXIMUM_TIME_WITHOUT_WRITE_ACCESS = 500; // 500 ms

	/**
	 * The minimum size the spilling has to grow to before it is closed after a read request has been issued (given in
	 * bytes).
	 */
	private static final long MINIMUM_FILE_SIZE = 4L * 1024L * 1024L; // 4 MB

	/**
	 * Indicates whether the {@link WritableByteChannel} is currently locked.
	 */
	private boolean writableChannelLocked = false;

	/**
	 * Indicates whether a read request has been issued for this file.
	 */
	private boolean readRequested = false;

	private final FileID fileID;

	/**
	 * The physical file which backs this spilling file.
	 */
	private final File physicalFile;

	/**
	 * The file channel used to write data to the file.
	 */
	private final FileChannel writableFileChannel;

	/**
	 * Time stamp of the last file channel unlock operation.
	 */
	private long lastUnlockTime = 0;

	/**
	 * The size of the spilling file after the last unlock operation.
	 */
	private long currentFileSize = 0;

	/**
	 * The number of file buffers backed by this writable spilling file.
	 */
	private int numberOfBuffers = 0;

	/**
	 * Constructs a new writable spilling file.
	 * 
	 * @param fileID
	 *        the ID of the file, must not be <code>null</code>
	 * @param physicalFile
	 *        the physical file which shall back this object, must not be <code>null</code>
	 * @throws IOException
	 *         thrown if the given file cannot be opened for writing
	 */
	WritableSpillingFile(final FileID fileID, final File physicalFile) throws IOException {

		if (fileID == null) {
			throw new IllegalArgumentException("Argument file ID must not be null");
		}

		if (physicalFile == null) {
			throw new IllegalArgumentException("Argument physical file must not be null");
		}

		this.fileID = fileID;
		this.physicalFile = physicalFile;
		this.writableFileChannel = new FileOutputStream(this.physicalFile).getChannel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		if (!this.readRequested) {
			throw new IOException("close called but no read access has been requested");
		}

		if (!this.writableFileChannel.isOpen()) {
			throw new IOException("writable file channel is already closed");
		}

		this.writableFileChannel.close();
	}

	/**
	 * Returns this physical file which backs this object.
	 * 
	 * @return the physical file which backs this object
	 */
	public File getPhysicalFile() {
		return this.physicalFile;
	}

	/**
	 * Checks if it is safe to close the file.
	 * 
	 * @return <code>true</code> if it safe to close the file, <code>false</code> otherwise
	 */
	public boolean isSafeToClose() {

		if (this.writableChannelLocked) {
			return false;
		}

		if (this.currentFileSize >= MINIMUM_FILE_SIZE) {
			return true;
		}

		if ((System.currentTimeMillis() - this.lastUnlockTime) > MAXIMUM_TIME_WITHOUT_WRITE_ACCESS) {
			return true;
		}

		return false;
	}

	/**
	 * Locks and returns this spilling file's {@link WritableByteChannel} for a write operation.
	 * 
	 * @return the byte channel if the lock operation has been successful, or <code>null</code> if the lock operation
	 *         failed
	 */
	FileChannel lockWritableFileChannel() {

		if (this.writableChannelLocked) {
			return null;
		}

		this.writableChannelLocked = true;

		return this.writableFileChannel;
	}

	/**
	 * Releases the lock on the spilling file's {@link WritableByteChannel}.
	 * 
	 * @param currentFileSize
	 *        the current size of the spilling file in bytes
	 */
	void unlockWritableFileChannel(final long currentFileSize) {

		this.writableChannelLocked = false;

		this.currentFileSize = currentFileSize;
		this.lastUnlockTime = System.currentTimeMillis();

		++this.numberOfBuffers;
	}

	/**
	 * Checks if a read request has been issued for this spilling file.
	 * 
	 * @return <code>true</code> if a read request has been issued, <code>false</code> otherwise
	 */
	boolean isReadRequested() {

		return this.readRequested;
	}

	/**
	 * Issues a read request to this spilling file.
	 */
	void requestReadAccess() {

		this.readRequested = true;
	}

	/**
	 * Returns the ID of the file which backs the file buffer.
	 * 
	 * @return the ID of the file which backs the file buffer
	 */
	FileID getFileID() {

		return this.fileID;
	}

	ReadableSpillingFile toReadableSpillingFile() throws IOException {

		return new ReadableSpillingFile(this.physicalFile, this.numberOfBuffers);
	}
}
