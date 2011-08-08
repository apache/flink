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
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.types.Record;
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

	private final Map<ChannelID, Object> channelGroupMap = new ConcurrentHashMap<ChannelID, Object>();

	private final Map<Object, WritableSpillingFile> writableSpillingFileMap = new HashMap<Object, WritableSpillingFile>();

	private final Map<Object, Queue<ReadableSpillingFile>> readableSpillingFileMap = new HashMap<Object, Queue<ReadableSpillingFile>>();

	private final Set<ChannelID> canceledChannels;

	public FileBufferManager(final Set<ChannelID> canceledChannels) {

		this.tmpDir = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);

		this.canceledChannels = canceledChannels;
	}

	public FileChannel getFileChannelForReading(final ChannelID sourceChannelID) throws IOException,
			InterruptedException {

		final Object groupObject = this.channelGroupMap.get(sourceChannelID);
		if (groupObject == null) {
			throw new IOException("Cannot find input gate for source channel ID " + sourceChannelID);
		}

		Queue<ReadableSpillingFile> queue = null;
		synchronized (this.readableSpillingFileMap) {
			queue = this.readableSpillingFileMap.get(groupObject);
			if (queue == null) {
				queue = new ArrayDeque<ReadableSpillingFile>(1);
				this.readableSpillingFileMap.put(groupObject, queue);
			}
		}

		ReadableSpillingFile readableSpillingFile = null;
		synchronized (queue) {

			while (queue.isEmpty()) {

				synchronized (this.writableSpillingFileMap) {
					WritableSpillingFile writableSpillingFile = this.writableSpillingFileMap.get(groupObject);
					if (writableSpillingFile != null) {
						writableSpillingFile.requestReadAccess();

						if (writableSpillingFile.isSafeToClose()) {
							writableSpillingFile.close();
							this.writableSpillingFileMap.remove(groupObject);
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

	public void reportFileBufferAsConsumed(final ChannelID sourceChannelID) {

		try {
			final Object groupObject = this.channelGroupMap.get(sourceChannelID);
			if (groupObject == null) {
				if (this.canceledChannels.contains(sourceChannelID)) {
					return;
				} else {
					throw new IOException("Cannot find input gate for source channel ID " + sourceChannelID);
				}
			}

			Queue<ReadableSpillingFile> queue = null;
			synchronized (this.readableSpillingFileMap) {
				queue = this.readableSpillingFileMap.get(groupObject);
				if (queue == null) {
					if(this.canceledChannels.contains(sourceChannelID)) {
						return;
					} else {
						throw new IOException("Cannot find readable spilling file queue for group object " + groupObject);
					}
				}

				ReadableSpillingFile readableSpillingFile = null;
				synchronized (queue) {
					readableSpillingFile = queue.peek();
					if (readableSpillingFile == null) {
						if (this.canceledChannels.contains(sourceChannelID)) {
							return;
						} else {
							throw new IOException("Cannot find readable spilling file for source channel "
								+ sourceChannelID);
						}
					}
					try {
						readableSpillingFile.unlockReadableFileChannel();
						if (readableSpillingFile.checkForEndOfFile()) {
							queue.poll();
							if (queue.isEmpty()) {
								this.readableSpillingFileMap.remove(groupObject);
							}
						}
					} catch (ClosedChannelException e) {
						if (this.canceledChannels.contains(sourceChannelID)) {
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
	 * @param sourceChannelID
	 *        the ID of the {@link AbstractByteBufferedOutputChannel} the file channel shall be locked for
	 * @return the file channel object if the lock could be acquired or <code>null</code> if the locking operation
	 *         failed
	 * @throws IOException
	 *         thrown if no spilling for the given channel ID could be allocated
	 * @throws ChannelCancelException
	 *         thrown to indicate that the input channel for which the data is written has been canceled
	 */
	public FileChannel getFileChannelForWriting(final ChannelID sourceChannelID) throws IOException,
			ChannelCanceledException {

		final Object groupObject = this.channelGroupMap.get(sourceChannelID);
		if (groupObject == null) {
			if (this.canceledChannels.contains(sourceChannelID)) {
				throw new ChannelCanceledException();
			} else {
				throw new IOException("Cannot find input gate for source channel ID " + sourceChannelID);
			}
		}

		synchronized (this.writableSpillingFileMap) {

			WritableSpillingFile writableSpillingFile = this.writableSpillingFileMap.get(groupObject);
			if (writableSpillingFile == null) {
				final String filename = this.tmpDir + File.separator + FileUtils.getRandomFilename("fb_");
				writableSpillingFile = new WritableSpillingFile(new File(filename));
				this.writableSpillingFileMap.put(groupObject, writableSpillingFile);
			}

			return writableSpillingFile.lockWritableFileChannel();
		}
	}

	/**
	 * Returns the lock for a file channel of a {@link WritableSpillingFile}.
	 * 
	 * @param sourceChannelID
	 *        the ID of the {@link AbstractByteBufferedOutputChannel} the lock has been acquired for
	 * @param currentFileSize
	 *        the size of the file after the last write operation using the locked file channel
	 * @throws IOException
	 *         thrown if the lock could not be released
	 */
	public void reportEndOfWritePhase(final ChannelID sourceChannelID, final long currentFileSize) throws IOException {

		final Object groupObject = this.channelGroupMap.get(sourceChannelID);
		if (groupObject == null) {
			if (this.canceledChannels.contains(sourceChannelID)) {
				return;
			} else {
				throw new IOException("Cannot find input gate for source channel ID " + sourceChannelID);
			}
		}

		WritableSpillingFile writableSpillingFile = null;
		boolean removed = false;
		synchronized (this.writableSpillingFileMap) {

			writableSpillingFile = this.writableSpillingFileMap.get(groupObject);
			if (writableSpillingFile == null) {
				throw new IOException("Cannot find writable spilling file for group object " + groupObject);
			}

			writableSpillingFile.unlockWritableFileChannel(currentFileSize);

			if (writableSpillingFile.isReadRequested() && writableSpillingFile.isSafeToClose()) {
				this.writableSpillingFileMap.remove(groupObject);
				removed = true;
			}
		}

		if (removed) {
			writableSpillingFile.close();
			Queue<ReadableSpillingFile> queue = null;
			synchronized (this.readableSpillingFileMap) {
				queue = this.readableSpillingFileMap.get(groupObject);
				if (queue == null) {
					queue = new ArrayDeque<ReadableSpillingFile>(1);
					this.readableSpillingFileMap.put(groupObject, queue);
				}
			}
			synchronized (queue) {
				queue.add(new ReadableSpillingFile(writableSpillingFile.getPhysicalFile()));
				queue.notify();
			}
		}
	}

	public void registerExternalDataSourceForChannel(final ChannelID sourceChannelID, final String filename)
			throws IOException {

		final Object groupObject = this.channelGroupMap.get(sourceChannelID);
		if (groupObject == null) {
			throw new IOException("Cannot find input gate for source channel ID " + sourceChannelID);
		}

		Queue<ReadableSpillingFile> queue = null;
		synchronized (this.readableSpillingFileMap) {

			queue = this.readableSpillingFileMap.get(groupObject);
			if (queue == null) {
				queue = new ArrayDeque<ReadableSpillingFile>(1);
				this.readableSpillingFileMap.put(groupObject, queue);
			}
		}

		synchronized (queue) {
			queue.add(new ReadableSpillingFile(new File(filename)));
			queue.notify();
		}
	}

	public void registerChannelToGateMapping(final ChannelID sourceChannelID,
			final InputGate<? extends Record> inputGate) {

		final Object previousGate = this.channelGroupMap.put(sourceChannelID, inputGate);
		if (previousGate != null) {
			LOG.error("Source channel ID has been previously registered to input gate " + inputGate.getJobID() + ", "
				+ inputGate.getIndex());
		}
	}
	public void registerChannelToOutGateMapping(final ChannelID sourceChannelID,
			final OutputGate<? extends Record> outputGate) {
		LOG.info("Register Mapping " + sourceChannelID);
		final Object previousGate = this.channelGroupMap.put(sourceChannelID, outputGate);
		if (previousGate != null) {
			LOG.error("Source channel ID has been previously registered to input gate " + outputGate.getJobID() + ", "
				+ outputGate.getIndex());
		}
	}
	public void unregisterChannelToGateMapping(final ChannelID sourceChannelID) {
		LOG.info("Unregister Mapping " + sourceChannelID);
		final Object groupObject = this.channelGroupMap.remove(sourceChannelID);
		if (groupObject == null) {
			LOG.error("Source channel ID has not been registered with any input gate");
		}

		boolean canceled = this.canceledChannels.contains(sourceChannelID);

		WritableSpillingFile writableSpillingFile = null;
		synchronized (this.writableSpillingFileMap) {
			writableSpillingFile = this.writableSpillingFileMap.remove(groupObject);
		}

		if (writableSpillingFile != null) {
			if (canceled) {
				try {
					writableSpillingFile.close();
					File file = writableSpillingFile.getPhysicalFile();
					if (file != null) {
						file.delete();
					}
				} catch (IOException ioe) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ioe));
					}
				}
			} else {
				LOG.error("There is still a writable spilling file for source channel " + sourceChannelID);
			}
		}

		Queue<ReadableSpillingFile> queue = null;
		synchronized (this.readableSpillingFileMap) {
			queue = this.readableSpillingFileMap.remove(groupObject);
		}

		if (queue != null) {
			if (canceled) {
				try {
					while (!queue.isEmpty()) {
						final ReadableSpillingFile rsf = queue.poll();
						if (rsf.isReadableChannelLocked()) {
							rsf.unlockReadableFileChannel();
						}
						final FileChannel fc = rsf.lockReadableFileChannel();
						fc.close();
						final File file = rsf.getPhysicalFile();
						if (file != null) {
							file.delete();
						}
					}
				} catch (IOException ioe) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ioe));
					}
				} catch (InterruptedException ie) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ie));
					}
				}
			} else {
				LOG.error("There is still " + queue.size() + " readable spilling file(s) for source channel "
					+ sourceChannelID);
			}
		}
	}
}
