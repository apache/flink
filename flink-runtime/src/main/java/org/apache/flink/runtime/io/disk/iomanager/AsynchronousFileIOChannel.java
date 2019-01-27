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
import org.apache.flink.runtime.util.NioBufferedFileOutputStream;
import org.apache.flink.runtime.util.NioBufferedFileInputStream;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base class for readers and writers that accept read or write requests for whole blocks.
 * The request is delegated to an asynchronous I/O thread. After completion of the I/O request, the memory
 * segment of the block is added to a collection to be returned.
 * <p>
 * The asynchrony of the access makes it possible to implement read-ahead or write-behind types of I/O accesses.
 *
 * @param <R> The type of request (e.g. <tt>ReadRequest</tt> or <tt>WriteRequest</tt> issued by this access to the I/O threads.
 */
public abstract class AsynchronousFileIOChannel<T, R extends IORequest> extends AbstractFileIOChannel {

	private final Object listenerLock = new Object();

	/**
	 * The lock that is used during closing to synchronize the thread that waits for all
	 * requests to be handled with the asynchronous I/O thread.
	 */
	protected final Object closeLock = new Object();

	/** A request queue for submitting asynchronous requests to the corresponding IO worker thread. */
	protected final RequestQueue<R> requestQueue;

	/** An atomic integer that counts the number of requests that we still wait for to return. */
	protected final AtomicInteger requestsNotReturned = new AtomicInteger(0);

	/** Handler for completed requests */
	protected final RequestDoneCallback<T> resultHandler;

	/** An exception that was encountered by the asynchronous request handling thread. */
	protected volatile IOException exception;

	/** Flag marking this channel as closed */
	protected volatile boolean closed;

	private NotificationListener allRequestsProcessedListener;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new channel access to the path indicated by the given ID. The channel accepts buffers to be
	 * read/written and hands them to the asynchronous I/O thread. After being processed, the buffers
	 * are returned by adding the to the given queue.
	 *
	 * @param channelID    The id describing the path of the file that the channel accessed.
	 * @param requestQueue The queue that this channel hands its IO requests to.
	 * @param callback     The callback to be invoked when a request is done.
	 * @param writeEnabled Flag describing whether the channel should be opened in read/write mode, rather
	 *                     than in read-only mode.
	 * @throws IOException Thrown, if the channel could no be opened.
	 */
	protected AsynchronousFileIOChannel(FileIOChannel.ID channelID, RequestQueue<R> requestQueue,
			RequestDoneCallback<T> callback, boolean writeEnabled) throws IOException
	{
		super(channelID, writeEnabled);

		this.requestQueue = checkNotNull(requestQueue);
		this.resultHandler = checkNotNull(callback);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	/**
	 * Closes the channel and waits until all pending asynchronous requests are processed. The
	 * underlying <code>FileChannel</code> is closed even if an exception interrupts the closing.
	 *
	 * <p> <strong>Important:</strong> the {@link #isClosed()} method returns <code>true</code>
	 * immediately after this method has been called even when there are outstanding requests.
	 *
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if
	 *                     the closing was interrupted.
	 */
	@Override
	public void close() throws IOException {
		// atomically set the close flag
		synchronized (this.closeLock) {
			if (this.closed) {
				return;
			}
			this.closed = true;

			try {
				// wait until as many buffers have been returned as were written
				// only then is everything guaranteed to be consistent.
				while (this.requestsNotReturned.get() > 0) {
					try {
						// we add a timeout here, because it is not guaranteed that the
						// decrementing during buffer return and the check here are deadlock free.
						// the deadlock situation is however unlikely and caught by the timeout
						this.closeLock.wait(1000);
						checkErroneous();
					}
					catch (InterruptedException iex) {
						throw new IOException("Closing of asynchronous file channel was interrupted.");
					}
				}

				// Additional check because we might have skipped the while loop
				checkErroneous();
			}
			finally {
				super.close();
			}
		}
	}

	/**
	 * This method waits for all pending asynchronous requests to return. When the
	 * last request has returned, the channel is closed and deleted.
	 * <p>
	 * Even if an exception interrupts the closing, such that not all request are handled,
	 * the underlying <tt>FileChannel</tt> is closed and deleted.
	 *
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if the closing was interrupted.
	 */
	@Override
	public void closeAndDelete() throws IOException {
		try {
			close();
		}
		finally {
			deleteChannel();
		}
	}

	/**
	 * Checks the exception state of this channel. The channel is erroneous, if one of its requests could not
	 * be processed correctly.
	 *
	 * @throws IOException Thrown, if the channel is erroneous. The thrown exception contains the original exception
	 *                     that defined the erroneous state as its cause.
	 */
	public final void checkErroneous() throws IOException {
		if (this.exception != null) {
			throw this.exception;
		}
	}

	/**
	 * Handles a processed <tt>Buffer</tt>. This method is invoked by the
	 * asynchronous IO worker threads upon completion of the IO request with the
	 * provided buffer and/or an exception that occurred while processing the request
	 * for that buffer.
	 *
	 * @param buffer The buffer to be processed.
	 * @param ex     The exception that occurred in the I/O threads when processing the buffer's request.
	 */
	final protected void handleProcessedBuffer(T buffer, IOException ex) {
		if (ex != null) {
			LOG.error("Process buffer request failed.", ex);
		}

		if (buffer == null) {
			return;
		}

		// even if the callbacks throw an error, we need to maintain our bookkeeping
		try {
			if (ex != null && this.exception == null) {
				this.exception = ex;
				this.resultHandler.requestFailed(buffer, ex);
			}
			else {
				this.resultHandler.requestSuccessful(buffer);
			}
		}
		finally {
			NotificationListener listener = null;

			// Decrement the number of outstanding requests. If we are currently closing, notify the
			// waiters. If there is a listener, notify her as well.
			synchronized (this.closeLock) {
				if (this.requestsNotReturned.decrementAndGet() == 0) {
					if (this.closed) {
						this.closeLock.notifyAll();
					}

					synchronized (listenerLock) {
						listener = allRequestsProcessedListener;
						allRequestsProcessedListener = null;
					}
				}
			}

			if (listener != null) {
				listener.onNotification();
			}
		}
	}

	final protected void addRequest(R request) throws IOException {
		// check the error state of this channel
		checkErroneous();

		// write the current buffer and get the next one
		this.requestsNotReturned.incrementAndGet();

		if (this.closed || this.requestQueue.isClosed()) {
			// if we found ourselves closed after the counter increment,
			// decrement the counter again and do not forward the request
			this.requestsNotReturned.decrementAndGet();

			final NotificationListener listener;

			synchronized (listenerLock) {
				listener = allRequestsProcessedListener;
				allRequestsProcessedListener = null;
			}

			if (listener != null) {
				listener.onNotification();
			}

			throw new IOException("I/O channel already closed (" + this.closed + "," + this.requestQueue.isClosed() + "). Could not fulfill: " + request);
		}

		this.requestQueue.add(request);
	}

	/**
	 * Registers a listener to be notified when all outstanding requests have been processed.
	 *
	 * <p> New requests can arrive right after the listener got notified. Therefore, it is not safe
	 * to assume that the number of outstanding requests is still zero after a notification unless
	 * there was a close right before the listener got called.
	 *
	 * <p> Returns <code>true</code>, if the registration was successful. A registration can fail,
	 * if there are no outstanding requests when trying to register a listener.
	 */
	protected boolean registerAllRequestsProcessedListener(NotificationListener listener) throws IOException {
		checkNotNull(listener);

		synchronized (listenerLock) {
			if (allRequestsProcessedListener == null) {
				// There was a race with the processing of the last outstanding request
				if (requestsNotReturned.get() == 0) {
					return false;
				}

				allRequestsProcessedListener = listener;

				return true;
			}
		}

		throw new IllegalStateException("Already subscribed.");
	}
}

//--------------------------------------------------------------------------------------------

/**
 * Read request that reads an entire memory segment from a block reader.
 */
final class SegmentReadRequest implements ReadRequest {

	private final AsynchronousFileIOChannel<MemorySegment, ReadRequest> channel;

	private final MemorySegment segment;
	private final int bufferSize;

	protected SegmentReadRequest(
			AsynchronousFileIOChannel<MemorySegment, ReadRequest> targetChannel,
			MemorySegment segment, int bufferSize) {
		this.bufferSize = bufferSize;
		if (segment == null) {
			throw new NullPointerException("Illegal read request with null memory segment.");
		}

		this.channel = targetChannel;
		this.segment = segment;
	}

	@Override
	public void read() throws IOException {
		try {
			if (bufferSize == -1) {
				final FileChannel c = this.channel.fileChannel;
				if (c.size() - c.position() > 0) {
					this.channel.fileChannel.read(this.segment.wrap(0, this.segment.size()));
				}
			} else {
				NioBufferedFileInputStream in = channel.getBufferedInputStream(bufferSize);
				if (in.available() > 0) {
					in.continuousRead(segment.wrap(0, this.segment.size()));
				}
			}
		} catch (NullPointerException ignored) {
			throw new IOException("Memory segment has been released.");
		}
	}

	@Override
	public void requestDone(IOException ioex) {
		this.channel.handleProcessedBuffer(this.segment, ioex);
	}
}

//--------------------------------------------------------------------------------------------

/**
 * Write request that writes an entire memory segment to the block writer.
 */
final class SegmentWriteRequest implements WriteRequest {

	private final AsynchronousFileIOChannel<MemorySegment, WriteRequest> channel;

	private final MemorySegment segment;
	private final int bufferSize;

	protected SegmentWriteRequest(AsynchronousFileIOChannel<MemorySegment, WriteRequest> targetChannel,
			MemorySegment segment, int bufferSize) {
		this.channel = targetChannel;
		this.segment = segment;
		this.bufferSize = bufferSize;
	}

	@Override
	public void write() throws IOException {
		try {
			if (bufferSize == -1) {
				FileUtils.writeCompletely(channel.fileChannel, segment.wrap(0, segment.size()));
			} else {
				channel.getBufferedOutputStream(bufferSize).write(segment, 0, segment.size());
			}
		} catch (NullPointerException npex) {
			throw new IOException("Memory segment has been released.");
		}
	}

	@Override
	public void requestDone(IOException ioex) {
		this.channel.handleProcessedBuffer(this.segment, ioex);
	}
}

final class BufferWriteRequest implements WriteRequest {

	private final AsynchronousFileIOChannel<Buffer, WriteRequest> channel;

	private final Buffer buffer;
	private final int bufferSize;

	protected BufferWriteRequest(AsynchronousFileIOChannel<Buffer, WriteRequest> targetChannel,
			Buffer buffer, int bufferSize) {
		this.channel = checkNotNull(targetChannel);
		this.buffer = checkNotNull(buffer);
		this.bufferSize = bufferSize;
	}

	@Override
	public void write() throws IOException {
		ByteBuffer nioBufferReadable = buffer.getNioBufferReadable();

		final ByteBuffer header = ByteBuffer.allocate(8);

		header.putInt(buffer.isBuffer() ? 1 : 0);
		header.putInt(nioBufferReadable.remaining());
		header.flip();

		if (bufferSize == -1) {
			FileUtils.writeCompletely(channel.fileChannel, header);
			FileUtils.writeCompletely(channel.fileChannel, nioBufferReadable);
		} else {
			NioBufferedFileOutputStream out = channel.getBufferedOutputStream(bufferSize);
			out.write(header);
			out.write(nioBufferReadable);
		}
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(buffer, error);
	}
}

final class BufferReadRequest implements ReadRequest {

	private final AsynchronousFileIOChannel<Buffer, ReadRequest> channel;

	private final Buffer buffer;

	private final AtomicBoolean hasReachedEndOfFile;

	private final int bufferSize;

	protected BufferReadRequest(AsynchronousFileIOChannel<Buffer, ReadRequest> targetChannel, Buffer buffer,
			AtomicBoolean hasReachedEndOfFile, int bufferSize) {
		this.channel = targetChannel;
		this.buffer = buffer;
		this.hasReachedEndOfFile = hasReachedEndOfFile;
		this.bufferSize = bufferSize;
	}

	@Override
	public void read() throws IOException {

		final FileChannel fileChannel = channel.fileChannel;

		if (bufferSize == -1) {
			if (fileChannel.size() - fileChannel.position() > 0) {
				BufferFileChannelReader reader = new BufferFileChannelReader(fileChannel);
				hasReachedEndOfFile.set(reader.readBufferFromFileChannel(buffer));
			} else {
				hasReachedEndOfFile.set(true);
			}
		} else {
			NioBufferedFileInputStream in = channel.getBufferedInputStream(bufferSize);
			if (in.available() > 0) {
				BufferFileChannelReader reader = new BufferFileChannelReader(fileChannel);
				hasReachedEndOfFile.set(reader.readBufferFromFileBufferedChannel(in, buffer));
			} else {
				hasReachedEndOfFile.set(true);
			}
		}
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(buffer, error);
	}
}

/**
 * Write requests that write a buffer to file in the form of stream.
 * Comparing to {@link BufferWriteRequest}, {@link StreamWriteRequest} gets rid of
 * the header field for each buffer, thus limits its usage to spill only non-event buffer.
 */
final class StreamWriteRequest implements WriteRequest {

	private final AsynchronousFileIOChannel<Buffer, WriteRequest> channel;

	private final Buffer buffer;
	private final int bufferSize;

	protected StreamWriteRequest(
		AsynchronousFileIOChannel<Buffer, WriteRequest> targetChannel,
		Buffer buffer, int bufferSize) {

		this.channel = checkNotNull(targetChannel);
		this.buffer = checkNotNull(buffer);
		this.bufferSize = bufferSize;
	}

	@Override
	public void write() throws IOException {
		checkArgument(buffer.isBuffer(), "Cannot write event buffer in StreamWriteRequest.");
		if (bufferSize == -1) {
			FileUtils.writeCompletely(channel.fileChannel, buffer.getNioBufferReadable());
		} else {
			NioBufferedFileOutputStream out = channel.getBufferedOutputStream(bufferSize);
			out.write(buffer.getNioBufferReadable());
		}
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(buffer, error);
	}
}

/**
 * Read request that read a sequence of data to a buffer, specified by the offset in the file
 * and the length of the data.
 */
final class StreamReadRequest implements ReadRequest {

	private final AsynchronousFileIOChannel<Buffer, ReadRequest> channel;

	private final Buffer buffer;

	private final AtomicBoolean hasReachedEndOfFile;

	private final long startPosition;

	private final long bytesToRead;

	private final int bufferSize;

	protected StreamReadRequest(
		AsynchronousFileIOChannel<Buffer, ReadRequest> targetChannel,
		Buffer buffer,
		AtomicBoolean hasReachedEndOfFile,
		long startPosition,
		long bytesToRead,
		int bufferSize) {

		this.channel = targetChannel;
		this.buffer = buffer;
		this.hasReachedEndOfFile = hasReachedEndOfFile;
		this.startPosition = startPosition;
		this.bytesToRead = bytesToRead;
		this.bufferSize = bufferSize;
	}

	@Override
	public void read() throws IOException {
		if (!hasReachedEndOfFile.get()) {
			if (bufferSize == -1) {
				hasReachedEndOfFile.set(StreamFileChannelReader.readBufferFromFileChannel(
					channel.fileChannel, buffer, startPosition, bytesToRead, channel.id.getPathFile()));
			} else {
				NioBufferedFileInputStream in = channel.getBufferedInputStream(bufferSize);
				if (in.available() > 0) {
					hasReachedEndOfFile.set(StreamFileChannelReader.readBufferFromFileBufferedChannel(
						in, buffer, startPosition, bytesToRead, channel.id.getPathFile()));
				} else {
					hasReachedEndOfFile.set(true);
				}
			}
		}
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(buffer, error);
	}
}

final class FileSegmentReadRequest implements ReadRequest {

	private final AsynchronousFileIOChannel<FileSegment, ReadRequest> channel;

	private final AtomicBoolean hasReachedEndOfFile;

	private FileSegment fileSegment;

	protected FileSegmentReadRequest(AsynchronousFileIOChannel<FileSegment, ReadRequest> targetChannel, AtomicBoolean hasReachedEndOfFile) {
		this.channel = targetChannel;
		this.hasReachedEndOfFile = hasReachedEndOfFile;
	}

	@Override
	public void read() throws IOException {

		final FileChannel fileChannel = channel.fileChannel;

		if (fileChannel.size() - fileChannel.position() > 0) {
			final ByteBuffer header = ByteBuffer.allocate(8);

			fileChannel.read(header);
			header.flip();

			final long position = fileChannel.position();

			final boolean isBuffer = header.getInt() == 1;
			final int length = header.getInt();

			fileSegment = new FileSegment(fileChannel, position, length, isBuffer);

			// Skip the binary data
			fileChannel.position(position + length);

			hasReachedEndOfFile.set(fileChannel.size() - fileChannel.position() == 0);
		}
		else {
			hasReachedEndOfFile.set(true);
		}
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(fileSegment, error);
	}
}

/**
 * Request that seeks the underlying file channel to the given position.
 */
final class SeekRequest implements ReadRequest, WriteRequest {

	private final AsynchronousFileIOChannel<?, ?> channel;
	private final long position;
	private final int bufferSize;

	protected SeekRequest(AsynchronousFileIOChannel<?, ?> channel, long position) {
		this(channel, position, -1);
	}

	protected SeekRequest(AsynchronousFileIOChannel<?, ?> channel, long position, int bufferSize) {
		this.channel = channel;
		this.position = position;
		this.bufferSize = bufferSize;
	}

	@Override
	public void requestDone(IOException ioex) {
	}

	@Override
	public void read() throws IOException {
		if (bufferSize != -1) {
			channel.getBufferedInputStream(bufferSize).clearBuffer();
		}
		this.channel.fileChannel.position(position);
	}

	@Override
	public void write() throws IOException {
		if (bufferSize != -1) {
			channel.getBufferedOutputStream(bufferSize).flush();
		}
		this.channel.fileChannel.position(position);
	}
}
