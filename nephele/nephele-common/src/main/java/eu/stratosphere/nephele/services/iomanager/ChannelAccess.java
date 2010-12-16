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

package eu.stratosphere.nephele.services.iomanager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A base class for reading from / writing to a file system file.
 * 
 * @author Alexander Alexandrov
 * @param <T>
 *        the buffer type used for the underlying IO operations
 */
public abstract class ChannelAccess<T extends Buffer> {
	/**
	 * The ID of the underlying channel.
	 */
	protected final Channel.ID id;

	/**
	 * The file described by the Channel.ID.
	 */
	protected final RandomAccessFile file;

	/**
	 * A file channel for NIO access to the file.
	 */
	protected final FileChannel fileChannel;

	/**
	 * A request queue for submitting asynchronous requests to the corresponding
	 * IO worker thread.
	 */
	protected final BlockingQueue<IORequest<T>> requestQueue;

	/**
	 * A reference to the current IO buffer.
	 */
	protected T currentBuffer;

	/**
	 * Reentrant lock used for synchronization between the ChannelAccess objects
	 * and the IO worker threads.
	 */
	protected final ReentrantLock lock;

	/**
	 * A condition variable indicating that all pending IO requests have been
	 * completed.
	 */
	protected final Condition allRequestsHandled;

	/**
	 * A boolean flag indicating that the {@link ChannelAccess#close()} method
	 * has already been invoked.
	 */
	protected boolean isClosing;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Constructor.
	 */
	protected ChannelAccess(Channel.ID channelID, BlockingQueue<IORequest<T>> requestQueue)
																							throws ServiceException {
		try {
			this.id = channelID;
			this.file = new RandomAccessFile(id.getPath(), "rw");
			this.fileChannel = this.file.getChannel();

			this.requestQueue = requestQueue;

			this.lock = new ReentrantLock();
			this.allRequestsHandled = lock.newCondition();
			this.isClosing = false;
		} catch (IOException e) {
			throw new ServiceException(e);
		}
	}

	/**
	 * Release resources and return the externally supplied memory segments.
	 * 
	 * @throws ServiceException
	 */
	public abstract Collection<MemorySegment> close() throws ServiceException;

	/**
	 * Handle a processed {@code buffer}. This method is invoked by the
	 * asynchronous IO worker threads upon completion of the IO request with the
	 * provided {@code buffer}.
	 * 
	 * @param buffer
	 */
	protected abstract void handleProcessedBuffer(T buffer);

	/**
	 * Deletes this channel by physically removing the file beneath it.
	 * This method may only be called on a closed channel.
	 */
	public void deleteChannel() {
		if (fileChannel.isOpen()) {
			throw new IllegalStateException("Cannot delete a channel that is open.");
		}

		// make a best effort to delete the file. Don't report exceptions
		try {
			File f = new File(this.id.getPath());
			if (f.exists()) {
				f.delete();
			}
		} catch (Throwable t) {
		}
	}

	/**
	 * An wrapper class for submitting IO requests to a centralized asynchronous
	 * IO worker thread. A request consists of an IO buffer to be processed and
	 * a reference to the {@link ChannelAccess} object submitting the request.
	 * 
	 * @author Alexander Alexandrov
	 * @param <T>
	 *        The buffer type for this request
	 */
	protected static final class IORequest<T extends Buffer> {
		protected final ChannelAccess<T> channelAccess;

		protected final T buffer;

		protected IORequest(ChannelAccess<T> channelWrapper, T buffer) {
			this.channelAccess = channelWrapper;
			this.buffer = buffer;
		}
	}
}
