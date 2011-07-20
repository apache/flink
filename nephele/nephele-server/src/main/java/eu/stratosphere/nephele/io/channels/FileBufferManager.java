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

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.util.FileUtils;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The file buffer manager manages the physical files which may be used to store the output or input of
 * {@link AbstractByteBufferedOutputChannel} or {@link AbstractByteBufferedInputChannel} objects, respectively. It is
 * designed as a singleton object.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class FileBufferManager {

	/**
	 * The logging object.
	 */
	private static final Log LOG = LogFactory.getLog(FileBufferManager.class);

	/**
	 * Stores the location of the directory for temporary files.
	 */
	private final String tmpDir;

	private final Map<GateID, WritableSpillingFile> writableSpillingFileMap = new HashMap<GateID, WritableSpillingFile>();

	private final Map<GateID, Queue<ReadableSpillingFile>> readableSpillingFileMap = new HashMap<GateID, Queue<ReadableSpillingFile>>();

	private final Set<ChannelID> canceledChannels;

	public FileBufferManager(final Set<ChannelID> canceledChannels) {

		this.tmpDir = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);

		this.canceledChannels = canceledChannels;
	}

	public FileChannel getFileChannelForReading(final GateID gateID) throws IOException,
			InterruptedException {

		Queue<ReadableSpillingFile> queue = null;
		synchronized (this.readableSpillingFileMap) {
			queue = this.readableSpillingFileMap.get(gateID);
			if (queue == null) {
				queue = new ArrayDeque<ReadableSpillingFile>(1);
				this.readableSpillingFileMap.put(gateID, queue);
			}
		}

		ReadableSpillingFile readableSpillingFile = null;
		synchronized (queue) {

			while (queue.isEmpty()) {

				synchronized (this.writableSpillingFileMap) {
					WritableSpillingFile writableSpillingFile = this.writableSpillingFileMap.get(gateID);
					if (writableSpillingFile != null) {
						writableSpillingFile.requestReadAccess();

						if (writableSpillingFile.isSafeToClose()) {
							writableSpillingFile.close();
							this.writableSpillingFileMap.remove(gateID);
							queue.add(new ReadableSpillingFile(writableSpillingFile.getPhysicalFile()));
						}
					}
				}

				if (queue.isEmpty()) {
					queue.wait(WritableSpillingFile.MAXIMUM_TIME_WITHOUT_WRITE_ACCESS);
				}
			}

			readableSpillingFile = queue.peek();
		}

		return readableSpillingFile.lockReadableFileChannel();
	}

	public void reportFileBufferAsConsumed(final GateID gateID) {

		try {
			Queue<ReadableSpillingFile> queue = null;
			synchronized (this.readableSpillingFileMap) {
				queue = this.readableSpillingFileMap.get(gateID);
				if (queue == null) {
					if (this.canceledChannels.contains(gateID)) {
						return;
					} else {
						throw new IOException("Cannot find readable spilling file queue for gate ID " + gateID);
					}
				}

				ReadableSpillingFile readableSpillingFile = null;
				synchronized (queue) {
					readableSpillingFile = queue.peek();
					if (readableSpillingFile == null) {
						if (this.canceledChannels.contains(gateID)) {
							return;
						} else {
							throw new IOException("Cannot find readable spilling file for gate ID " + gateID);
						}
					}
					try {
						readableSpillingFile.unlockReadableFileChannel();
						if (readableSpillingFile.checkForEndOfFile()) {
							queue.poll();
							if (queue.isEmpty()) {
								this.readableSpillingFileMap.remove(gateID);
							}
						}
					} catch (ClosedChannelException e) {
						if (this.canceledChannels.contains(gateID)) {
							// The user thread has been interrupted
							readableSpillingFile.getPhysicalFile().delete();
						} else {
							throw e; // This is actually an exception
						}
					}
				}
			}

		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}
	}

	/**
	 * Locks and returns a file channel from a {@link WritableSpillingFile}.
	 * 
	 * @param gateID
	 *        the ID of the gate the file channel shall be locked for
	 * @return the file channel object if the lock could be acquired or <code>null</code> if the locking operation
	 *         failed
	 * @throws IOException
	 *         thrown if no spilling for the given channel ID could be allocated
	 * @throws ChannelCancelException
	 *         thrown to indicate that the input channel for which the data is written has been canceled
	 */
	public FileChannel getFileChannelForWriting(final GateID gateID) throws IOException,
			ChannelCanceledException {

		synchronized (this.writableSpillingFileMap) {

			WritableSpillingFile writableSpillingFile = this.writableSpillingFileMap.get(gateID);
			if (writableSpillingFile == null) {
				final String filename = this.tmpDir + File.separator + FileUtils.getRandomFilename("fb_");
				writableSpillingFile = new WritableSpillingFile(new File(filename));
				this.writableSpillingFileMap.put(gateID, writableSpillingFile);
			}

			return writableSpillingFile.lockWritableFileChannel();
		}
	}

	/**
	 * Returns the lock for a file channel of a {@link WritableSpillingFile}.
	 * 
	 * @param gateID
	 *        the ID of the gate the lock has been acquired for
	 * @param currentFileSize
	 *        the size of the file after the last write operation using the locked file channel
	 * @throws IOException
	 *         thrown if the lock could not be released
	 */
	public void reportEndOfWritePhase(final GateID gateID, final long currentFileSize) throws IOException {

		WritableSpillingFile writableSpillingFile = null;
		boolean removed = false;
		synchronized (this.writableSpillingFileMap) {

			writableSpillingFile = this.writableSpillingFileMap.get(gateID);
			if (writableSpillingFile == null) {
				throw new IOException("Cannot find writable spilling file for gate ID " + gateID);
			}

			writableSpillingFile.unlockWritableFileChannel(currentFileSize);

			if (writableSpillingFile.isReadRequested() && writableSpillingFile.isSafeToClose()) {
				this.writableSpillingFileMap.remove(gateID);
				removed = true;
			}
		}

		if (removed) {
			writableSpillingFile.close();
			Queue<ReadableSpillingFile> queue = null;
			synchronized (this.readableSpillingFileMap) {
				queue = this.readableSpillingFileMap.get(gateID);
				if (queue == null) {
					queue = new ArrayDeque<ReadableSpillingFile>(1);
					this.readableSpillingFileMap.put(gateID, queue);
				}
			}
			synchronized (queue) {
				queue.add(new ReadableSpillingFile(writableSpillingFile.getPhysicalFile()));
				queue.notify();
			}
		}
	}
}