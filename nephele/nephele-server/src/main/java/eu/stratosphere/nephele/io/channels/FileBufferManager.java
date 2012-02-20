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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;

/**
 * The file buffer manager manages the physical files which may be used to store the output or input of
 * {@link AbstractByteBufferedOutputChannel} or {@link AbstractByteBufferedInputChannel} objects, respectively. It is
 * designed as a singleton object.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 * @author Stephan Ewen
 */
public final class FileBufferManager {
	/**
	 * The prefix with which spill files are stored.
	 */
	public static final String FILE_BUFFER_PREFIX = "fb_";

	/**
	 * The logging object.
	 */
	private static final Log LOG = LogFactory.getLog(FileBufferManager.class);

	/**
	 * The singleton instance of the file buffer manager.
	 */
	private static final FileBufferManager instance = new FileBufferManager();

	/**
	 * Gets the singleton instance of the file buffer manager.
	 * 
	 * @return the file buffer manager singleton instance
	 */
	public static FileBufferManager getInstance() {
		return instance;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * The map from owner IDs to files
	 */
	private final ConcurrentHashMap<AbstractID, ChannelWithAccessInfo> fileMap;

	/**
	 * The directories for temporary files.
	 */
	private final String[] tmpDirs;

	/**
	 * Constructs a new file buffer manager object.
	 */
	private FileBufferManager() {

		this.tmpDirs = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(":");

		// check temp dirs
		for (int i = 0; i < this.tmpDirs.length; i++) {
			File f = new File(this.tmpDirs[i]);
			if (!(f.exists() && f.isDirectory() && f.canWrite())) {
				LOG.error("Temp directory '" + f.getAbsolutePath() + "' is not a writable directory. " +
					"Replacing path with default temp directory: " + ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
				this.tmpDirs[i] = ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH;
			}
			this.tmpDirs[i] = this.tmpDirs[i] + File.separator + FILE_BUFFER_PREFIX;
		}

		this.fileMap = new ConcurrentHashMap<AbstractID, ChannelWithAccessInfo>(2048, 0.8f, 64);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the file channel to for the owner with the given id.
	 * 
	 * @param id
	 *        The id for which to retrieve the channel.
	 * @throws IllegalStateException
	 *         Thrown, if the channel has not been registered or has already been removed.
	 */
	public FileChannel getChannel(final AbstractID id) throws IOException {

		final ChannelWithAccessInfo info = getChannelInternal(id, false);
		if (info != null) {
			return info.getChannel();
		} else {
			throw new IllegalStateException("No channel is registered (any more) for the given id.");
		}
	}

	/**
	 * Gets the file channel to for the owner with the given id and increments the references to that channel by one.
	 * 
	 * @param id
	 *        The id for which to retrieve the channel.
	 * @throws IllegalStateException
	 *         Thrown, if the channel has not been registered or has already been removed.
	 */
	public FileChannel getChannelAndIncrementReferences(final AbstractID owner) throws IOException {

		final ChannelWithAccessInfo info = getChannelInternal(owner, false);
		if (info != null) {
			return info.getAndIncrementReferences();
		} else {
			throw new IllegalStateException("No channel is registered (any more) for the given id.");
		}
	}

	/**
	 * Gets the file channel to for the owner with the given id and reserved the portion of the given size for
	 * writing. The position where the reserved space starts is contained in the return value. This method
	 * automatically increments the number of references to the channel by one.
	 * <p>
	 * This method always returns a channel. If no channel exists (yet or any more) for the given id, one is created.
	 * 
	 * @param id
	 *        The id for which to get the channel and reserve space.
	 */
	public ChannelWithPosition getChannelForWriteAndIncrementReferences(final AbstractID id, final int spaceToReserve)
			throws IOException {

		ChannelWithPosition c = null;
		do {
			// the return value may be zero, if someone asynchronously decremented the counter to zero
			// and caused the disposal of the channel. falling through the loop will create a
			// new channel.
			c = getChannelInternal(id, true).reserveWriteSpaceAndIncrementReferences(spaceToReserve);
		} while (c == null);

		return c;
	}

	/**
	 * Increments the references to the given channel.
	 * 
	 * @param id
	 *        The channel to increment the references for.
	 * @throws IllegalStateException
	 *         Thrown, if the channel has not been registered or has already been removed.
	 */
	public void incrementReferences(final AbstractID id) {

		ChannelWithAccessInfo entry = this.fileMap.get(id);
		if (entry == null || !entry.incrementReferences()) {
			throw new IllegalStateException("No channel is registered (any more) for the given id.");
		}
	}

	/**
	 * Decrements the references to the given channel. If the channel reaches zero references, it will be removed.
	 * 
	 * @param id
	 *        The channel to decrement the references for.
	 * @throws IllegalStateException
	 *         Thrown, if the channel has not been registered or has already been removed.
	 */
	public void decrementReferences(final AbstractID id) {

		ChannelWithAccessInfo entry = this.fileMap.get(id);
		if (entry != null) {
			if (entry.decrementReferences() <= 0) {
				this.fileMap.remove(id);
			}
		} else {
			throw new IllegalStateException("Channel is not (or no longer) registered at the file buffer manager.");
		}
	}

	// --------------------------------------------------------------------------------------------

	private final ChannelWithAccessInfo getChannelInternal(final AbstractID id, final boolean createIfAbsent)
			throws IOException {

		ChannelWithAccessInfo cwa = this.fileMap.get(id);
		if (cwa == null) {
			if (createIfAbsent) {

				// Construct the filename
				final int dirIndex = Math.abs(id.hashCode()) % this.tmpDirs.length;
				final File file = new File(this.tmpDirs[dirIndex] + id.toString());

				cwa = new ChannelWithAccessInfo(file);
				final ChannelWithAccessInfo alreadyContained = this.fileMap.putIfAbsent(id, cwa);
				if (alreadyContained != null) {
					// we had a race (should be a very rare event) and have created an
					// unneeded channel. dispose it and use the already contained one.
					cwa.disposeSilently();
					cwa = alreadyContained;
				}
			} else {
				return null;
			}
		}

		return cwa;
	}

	// --------------------------------------------------------------------------------------------

	private static final class ChannelWithAccessInfo {

		private final File file;

		private final FileChannel channel;

		private final AtomicLong reservedWritePosition;

		private final AtomicInteger referenceCounter;

		private ChannelWithAccessInfo(final File file) throws IOException {

			this.file = file;
			this.channel = new RandomAccessFile(file, "rw").getChannel();
			this.reservedWritePosition = new AtomicLong(0);
			this.referenceCounter = new AtomicInteger(0);
		}

		FileChannel getChannel() {

			return this.channel;
		}

		FileChannel getAndIncrementReferences() {

			if (incrementReferences()) {
				return this.channel;
			} else {
				return null;
			}
		}

		ChannelWithPosition reserveWriteSpaceAndIncrementReferences(final int spaceToReserve) {

			if (incrementReferences()) {
				return new ChannelWithPosition(this.channel, this.reservedWritePosition.getAndAdd(spaceToReserve));
			} else {
				return null;
			}
		}

		/**
		 * Decrements the number of references to this channel. If the number of references is zero after the
		 * decrement, the channel is deleted.
		 * 
		 * @return The number of references remaining after the decrement.
		 * @throws IllegalStateException
		 *         Thrown, if the number of references is already zero or below.
		 */
		int decrementReferences() {

			int current = this.referenceCounter.get();
			while (true) {
				if (current <= 0) {
					// this is actually an error case, because the channel was deleted before
					throw new IllegalStateException("The references to the file were already at zero.");
				}

				if (current == 1) {
					// this call decrements to zero, so mark it as deleted
					if (this.referenceCounter.compareAndSet(current, Integer.MIN_VALUE)) {
						current = 0;
						break;
					}
				} else if (this.referenceCounter.compareAndSet(current, current - 1)) {
					current = current - 1;
					break;
				}
				current = this.referenceCounter.get();
			}

			if (current > 0) {
				return current;
			} else if (current == 0) {
				// delete the channel
				this.referenceCounter.set(Integer.MIN_VALUE);
				this.reservedWritePosition.set(Long.MIN_VALUE);
				try {
					this.channel.close();
				} catch (IOException ioex) {
					if (FileBufferManager.LOG.isErrorEnabled())
						FileBufferManager.LOG.error("Error while closing spill file for file buffers: " +
							ioex.getMessage(), ioex);
				}
				this.file.delete();
				return current;
			} else {
				throw new IllegalStateException("The references to the file were already at zero.");
			}
		}

		/**
		 * Increments the references to this channel. Returns <code>true</code>, if successful, and <code>false</code>,
		 * if the channel has been disposed in the meantime.
		 * 
		 * @return True, if successful, false, if the channel has been disposed.
		 */
		boolean incrementReferences() {

			int current = this.referenceCounter.get();
			while (true) {
				// check whether it was disposed in the meantime
				if (current < 0) {
					return false;
				}
				// atomically check and increment
				if (this.referenceCounter.compareAndSet(current, current + 1)) {
					return true;
				}
				current = this.referenceCounter.get();
			}
		}

		/**
		 * Disposes the channel without further notice. Tries to close it (swallowing all exceptions) and tries
		 * to delete the file.
		 */
		void disposeSilently() {

			this.referenceCounter.set(Integer.MIN_VALUE);
			this.reservedWritePosition.set(Long.MIN_VALUE);

			if (this.channel.isOpen()) {
				try {
					this.channel.close();
				} catch (Throwable t) {
				}
			}
			this.file.delete();
		}
	}
}