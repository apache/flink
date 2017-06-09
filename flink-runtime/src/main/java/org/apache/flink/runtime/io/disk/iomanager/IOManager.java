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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The facade for the provided I/O manager services.
 */
public abstract class IOManager {

	public enum IOMode {

		SYNC(true), ASYNC(false);

		private final boolean isSynchronous;

		IOMode(boolean isSynchronous) {
			this.isSynchronous = isSynchronous;
		}

		public boolean isSynchronous() {
			return isSynchronous;
		}
	}

	/** Logging */
	protected static final Logger LOG = LoggerFactory.getLogger(IOManager.class);

	/** The temporary directories for files */
	private final File[] paths;

	/** A random number generator for the anonymous ChannelIDs. */
	private final Random random;

	/** The number of the next path to use. */
	private volatile int nextPath;

	// -------------------------------------------------------------------------
	//               Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Constructs a new IOManager.
	 *
	 * @param tempDirs The basic directories for files underlying anonymous channels.
	 */
	protected IOManager(String[] tempDirs) {
		if (tempDirs == null || tempDirs.length == 0) {
			throw new IllegalArgumentException("The temporary directories must not be null or empty.");
		}

		this.random = new Random();
		this.nextPath = 0;

		this.paths = new File[tempDirs.length];
		for (int i = 0; i < tempDirs.length; i++) {
			File baseDir = new File(tempDirs[i]);
			String subfolder = String.format("flink-io-%s", UUID.randomUUID().toString());
			File storageDir = new File(baseDir, subfolder);

			if (!storageDir.exists() && !storageDir.mkdirs()) {
				throw new RuntimeException(
						"Could not create storage directory for IOManager: " + storageDir.getAbsolutePath());
			}
			paths[i] = storageDir;
			LOG.info("I/O manager uses directory {} for spill files.", storageDir.getAbsolutePath());
		}
	}

	/**
	 * Close method, marks the I/O manager as closed
	 * and removed all temporary files.
	 */
	public void shutdown() {
		// remove all of our temp directories
		for (File path : paths) {
			try {
				if (path != null) {
					if (path.exists()) {
						FileUtils.deleteDirectory(path);
						LOG.info("I/O manager removed spill file directory {}", path.getAbsolutePath());
					}
				}
			} catch (Throwable t) {
				LOG.error("IOManager failed to properly clean up temp file directory: " + path, t);
			}
		}
	}

	/**
	 * Utility method to check whether the IO manager has been properly shut down.
	 * For this base implementation, this means that all files have been removed.
	 *
	 * @return True, if the IO manager has properly shut down, false otherwise.
	 */
	public boolean isProperlyShutDown() {
		for (File path : paths) {
			if (path != null && path.exists()) {
				return false;
			}
		}
		return true;
	}

	// ------------------------------------------------------------------------
	//                          Channel Instantiations
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@link FileIOChannel.ID} in one of the temp directories. Multiple
	 * invocations of this method spread the channels evenly across the different directories.
	 *
	 * @return A channel to a temporary directory.
	 */
	public FileIOChannel.ID createChannel() {
		final int num = getNextPathNum();
		return new FileIOChannel.ID(this.paths[num], num, this.random);
	}

	/**
	 * Creates a new {@link FileIOChannel.Enumerator}, spreading the channels in a round-robin fashion
	 * across the temporary file directories.
	 *
	 * @return An enumerator for channels.
	 */
	public FileIOChannel.Enumerator createChannelEnumerator() {
		return new FileIOChannel.Enumerator(this.paths, this.random);
	}

	/**
	 * Deletes the file underlying the given channel. If the channel is still open, this
	 * call may fail.
	 * 
	 * @param channel The channel to be deleted.
	 * @throws IOException Thrown if the deletion fails.
	 */
	public void deleteChannel(FileIOChannel.ID channel) throws IOException {
		if (channel != null) {
			if (channel.getPathFile().exists() && !channel.getPathFile().delete()) {
				LOG.warn("IOManager failed to delete temporary file {}", channel.getPath());
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//                        Reader / Writer instantiations
	// ------------------------------------------------------------------------

	/**
	 * Creates a block channel writer that writes to the given channel. The writer adds the
	 * written segment to its return-queue afterwards (to allow for asynchronous implementations).
	 *
	 * @param channelID The descriptor for the channel to write to.
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public BlockChannelWriter<MemorySegment> createBlockChannelWriter(FileIOChannel.ID channelID) throws IOException {
		return createBlockChannelWriter(channelID, new LinkedBlockingQueue<MemorySegment>());
	}

	/**
	 * Creates a block channel writer that writes to the given channel. The writer adds the
	 * written segment to the given queue (to allow for asynchronous implementations).
	 *
	 * @param channelID The descriptor for the channel to write to.
	 * @param returnQueue The queue to put the written buffers into.
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public abstract BlockChannelWriter<MemorySegment> createBlockChannelWriter(FileIOChannel.ID channelID,
				LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException;

	/**
	 * Creates a block channel writer that writes to the given channel. The writer calls the given callback
	 * after the I/O operation has been performed (successfully or unsuccessfully), to allow
	 * for asynchronous implementations.
	 *
	 * @param channelID The descriptor for the channel to write to.
	 * @param callback The callback to be called for
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public abstract BlockChannelWriterWithCallback<MemorySegment> createBlockChannelWriter(FileIOChannel.ID channelID, RequestDoneCallback<MemorySegment> callback) throws IOException;

	/**
	 * Creates a block channel reader that reads blocks from the given channel. The reader pushed
	 * full memory segments (with the read data) to its "return queue", to allow for asynchronous read
	 * implementations.
	 *
	 * @param channelID The descriptor for the channel to write to.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public BlockChannelReader<MemorySegment> createBlockChannelReader(FileIOChannel.ID channelID) throws IOException {
		return createBlockChannelReader(channelID, new LinkedBlockingQueue<MemorySegment>());
	}

	/**
	 * Creates a block channel reader that reads blocks from the given channel. The reader pushes the full segments
	 * to the given queue, to allow for asynchronous implementations.
	 *
	 * @param channelID The descriptor for the channel to write to.
	 * @param returnQueue The queue to put the full buffers into.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public abstract BlockChannelReader<MemorySegment> createBlockChannelReader(FileIOChannel.ID channelID,
										LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException;

	public abstract BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException;

	public abstract BufferFileReader createBufferFileReader(FileIOChannel.ID channelID, RequestDoneCallback<Buffer> callback) throws IOException;

	public abstract BufferFileSegmentReader createBufferFileSegmentReader(FileIOChannel.ID channelID, RequestDoneCallback<FileSegment> callback) throws IOException;

	/**
	 * Creates a block channel reader that reads all blocks from the given channel directly in one bulk.
	 * The reader draws segments to read the blocks into from a supplied list, which must contain as many
	 * segments as the channel has blocks. After the reader is done, the list with the full segments can be
	 * obtained from the reader.
	 * <p>
	 * If a channel is not to be read in one bulk, but in multiple smaller batches, a
	 * {@link BlockChannelReader} should be used.
	 *
	 * @param channelID The descriptor for the channel to write to.
	 * @param targetSegments The list to take the segments from into which to read the data.
	 * @param numBlocks The number of blocks in the channel to read.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public abstract BulkBlockChannelReader createBulkBlockChannelReader(FileIOChannel.ID channelID,
			List<MemorySegment> targetSegments, int numBlocks) throws IOException;


	// ------------------------------------------------------------------------
	//                          Utilities
	// ------------------------------------------------------------------------
	
	/**
	 * Gets the number of directories across which the I/O manager rotates its files.
	 * 
	 * @return The number of temporary file directories.
	 */
	public int getNumberOfSpillingDirectories() {
		return this.paths.length;
	}

	/**
	 * Gets the directories that the I/O manager spills to.
	 * 
	 * @return The directories that the I/O manager spills to.
	 */
	public File[] getSpillingDirectories() {
		return this.paths;
	}

	/**
	 * Gets the directories that the I/O manager spills to, as path strings.
	 *
	 * @return The directories that the I/O manager spills to, as path strings.
	 */
	public String[] getSpillingDirectoriesPaths() {
		String[] strings = new String[this.paths.length];
		for (int i = 0; i < strings.length; i++) {
			strings[i] = paths[i].getAbsolutePath();
		}
		return strings;
	}
	
	protected int getNextPathNum() {
		final int next = this.nextPath;
		final int newNext = next + 1;
		this.nextPath = newNext >= this.paths.length ? 0 : newNext;
		return next;
	}
}
