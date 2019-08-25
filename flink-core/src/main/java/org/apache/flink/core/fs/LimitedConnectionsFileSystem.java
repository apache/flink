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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A file system that limits the number of concurrently open input streams,
 * output streams, and total streams for a target file system.
 *
 * <p>This file system can wrap another existing file system in cases where
 * the target file system cannot handle certain connection spikes and connections
 * would fail in that case. This happens, for example, for very small HDFS clusters
 * with few RPC handlers, when a large Flink job tries to build up many connections during
 * a checkpoint.
 *
 * <p>The filesystem may track the progress of streams and close streams that have been
 * inactive for too long, to avoid locked streams of taking up the complete pool.
 * Rather than having a dedicated reaper thread, the calls that try to open a new stream
 * periodically check the currently open streams once the limit of open streams is reached.
 */
@Internal
public class LimitedConnectionsFileSystem extends FileSystem {

	private static final Logger LOG = LoggerFactory.getLogger(LimitedConnectionsFileSystem.class);

	/** The original file system to which connections are limited. */
	private final FileSystem originalFs;

	/** The lock that synchronizes connection bookkeeping. */
	private final ReentrantLock lock;

	/** Condition for threads that are blocking on the availability of new connections. */
	private final Condition available;

	/** The maximum number of concurrently open output streams. */
	private final int maxNumOpenOutputStreams;

	/** The maximum number of concurrently open input streams. */
	private final int maxNumOpenInputStreams;

	/** The maximum number of concurrently open streams (input + output). */
	private final int maxNumOpenStreamsTotal;

	/** The nanoseconds that a opening a stream may wait for availability. */
	private final long streamOpenTimeoutNanos;

	/** The nanoseconds that a stream may spend not writing any bytes before it is closed as inactive. */
	private final long streamInactivityTimeoutNanos;

	/** The set of currently open output streams. */
	@GuardedBy("lock")
	private final HashSet<OutStream> openOutputStreams;

	/** The set of currently open input streams. */
	@GuardedBy("lock")
	private final HashSet<InStream> openInputStreams;

	/** The number of output streams reserved to be opened. */
	@GuardedBy("lock")
	private int numReservedOutputStreams;

	/** The number of input streams reserved to be opened. */
	@GuardedBy("lock")
	private int numReservedInputStreams;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new output connection limiting file system.
	 *
	 * <p>If streams are inactive (meaning not writing bytes) for longer than the given timeout,
	 * then they are terminated as "inactive", to prevent that the limited number of connections gets
	 * stuck on only blocked threads.
	 *
	 * @param originalFs              The original file system to which connections are limited.
	 * @param maxNumOpenStreamsTotal  The maximum number of concurrent open streams (0 means no limit).
	 */
	public LimitedConnectionsFileSystem(FileSystem originalFs, int maxNumOpenStreamsTotal) {
		this(originalFs, maxNumOpenStreamsTotal, 0, 0);
	}

	/**
	 * Creates a new output connection limiting file system.
	 *
	 * <p>If streams are inactive (meaning not writing bytes) for longer than the given timeout,
	 * then they are terminated as "inactive", to prevent that the limited number of connections gets
	 * stuck on only blocked threads.
	 *
	 * @param originalFs              The original file system to which connections are limited.
	 * @param maxNumOpenStreamsTotal  The maximum number of concurrent open streams (0 means no limit).
	 * @param streamOpenTimeout       The maximum number of milliseconds that the file system will wait when
	 *                                no more connections are currently permitted.
	 * @param streamInactivityTimeout The milliseconds that a stream may spend not writing any
	 *                                bytes before it is closed as inactive.
	 */
	public LimitedConnectionsFileSystem(
			FileSystem originalFs,
			int maxNumOpenStreamsTotal,
			long streamOpenTimeout,
			long streamInactivityTimeout) {
		this(originalFs, maxNumOpenStreamsTotal, 0, 0, streamOpenTimeout, streamInactivityTimeout);
	}

	/**
	 * Creates a new output connection limiting file system, limiting input and output streams with
	 * potentially different quotas.
	 *
	 * <p>If streams are inactive (meaning not writing bytes) for longer than the given timeout,
	 * then they are terminated as "inactive", to prevent that the limited number of connections gets
	 * stuck on only blocked threads.
	 *
	 * @param originalFs              The original file system to which connections are limited.
	 * @param maxNumOpenStreamsTotal  The maximum number of concurrent open streams (0 means no limit).
	 * @param maxNumOpenOutputStreams The maximum number of concurrent open output streams (0 means no limit).
	 * @param maxNumOpenInputStreams  The maximum number of concurrent open input streams (0 means no limit).
	 * @param streamOpenTimeout       The maximum number of milliseconds that the file system will wait when
	 *                                no more connections are currently permitted.
	 * @param streamInactivityTimeout The milliseconds that a stream may spend not writing any
	 *                                bytes before it is closed as inactive.
	 */
	public LimitedConnectionsFileSystem(
			FileSystem originalFs,
			int maxNumOpenStreamsTotal,
			int maxNumOpenOutputStreams,
			int maxNumOpenInputStreams,
			long streamOpenTimeout,
			long streamInactivityTimeout) {

		checkArgument(maxNumOpenStreamsTotal >= 0, "maxNumOpenStreamsTotal must be >= 0");
		checkArgument(maxNumOpenOutputStreams >= 0, "maxNumOpenOutputStreams must be >= 0");
		checkArgument(maxNumOpenInputStreams >= 0, "maxNumOpenInputStreams must be >= 0");
		checkArgument(streamOpenTimeout >= 0, "stream opening timeout must be >= 0 (0 means infinite timeout)");
		checkArgument(streamInactivityTimeout >= 0, "stream inactivity timeout must be >= 0 (0 means infinite timeout)");

		this.originalFs = checkNotNull(originalFs, "originalFs");
		this.lock = new ReentrantLock(true);
		this.available = lock.newCondition();
		this.openOutputStreams = new HashSet<>();
		this.openInputStreams = new HashSet<>();
		this.maxNumOpenStreamsTotal = maxNumOpenStreamsTotal;
		this.maxNumOpenOutputStreams = maxNumOpenOutputStreams;
		this.maxNumOpenInputStreams = maxNumOpenInputStreams;

		// assign nanos overflow aware
		final long openTimeoutNanos = streamOpenTimeout * 1_000_000;
		final long inactivityTimeoutNanos = streamInactivityTimeout * 1_000_000;

		this.streamOpenTimeoutNanos =
				openTimeoutNanos >= streamOpenTimeout ? openTimeoutNanos : Long.MAX_VALUE;

		this.streamInactivityTimeoutNanos =
				inactivityTimeoutNanos >= streamInactivityTimeout ? inactivityTimeoutNanos : Long.MAX_VALUE;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the maximum number of concurrently open output streams.
	 */
	public int getMaxNumOpenOutputStreams() {
		return maxNumOpenOutputStreams;
	}

	/**
	 * Gets the maximum number of concurrently open input streams.
	 */
	public int getMaxNumOpenInputStreams() {
		return maxNumOpenInputStreams;
	}

	/**
	 * Gets the maximum number of concurrently open streams (input + output).
	 */
	public int getMaxNumOpenStreamsTotal() {
		return maxNumOpenStreamsTotal;
	}

	/**
	 * Gets the number of milliseconds that a opening a stream may wait for availability in the
	 * connection pool.
	 */
	public long getStreamOpenTimeout() {
		return streamOpenTimeoutNanos / 1_000_000;
	}

	/**
	 * Gets the milliseconds that a stream may spend not writing any bytes before it is closed as inactive.
	 */
	public long getStreamInactivityTimeout() {
		return streamInactivityTimeoutNanos / 1_000_000;
	}

	/**
	 * Gets the total number of open streams (input plus output).
	 */
	public int getTotalNumberOfOpenStreams() {
		lock.lock();
		try {
			return numReservedOutputStreams + numReservedInputStreams;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Gets the number of currently open output streams.
	 */
	public int getNumberOfOpenOutputStreams() {
		lock.lock();
		try {
			return numReservedOutputStreams;
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Gets the number of currently open input streams.
	 */
	public int getNumberOfOpenInputStreams() {
		return numReservedInputStreams;
	}

	// ------------------------------------------------------------------------
	//  input & output stream opening methods
	// ------------------------------------------------------------------------

	@Override
	public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
		return createOutputStream(() -> originalFs.create(f, overwriteMode));
	}

	@Override
	@Deprecated
	@SuppressWarnings("deprecation")
	public FSDataOutputStream create(
			Path f,
			boolean overwrite,
			int bufferSize,
			short replication,
			long blockSize) throws IOException {

		return createOutputStream(() -> originalFs.create(f, overwrite, bufferSize, replication, blockSize));
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		return createInputStream(() -> originalFs.open(f, bufferSize));
	}

	@Override
	public FSDataInputStream open(Path f) throws IOException {
		return createInputStream(() -> originalFs.open(f));
	}

	private FSDataOutputStream createOutputStream(
			final SupplierWithException<FSDataOutputStream, IOException> streamOpener) throws IOException {

		final SupplierWithException<OutStream, IOException> wrappedStreamOpener =
				() -> new OutStream(streamOpener.get(), this);

		return createStream(wrappedStreamOpener, openOutputStreams, true);
	}

	private FSDataInputStream createInputStream(
			final SupplierWithException<FSDataInputStream, IOException> streamOpener) throws IOException {

		final SupplierWithException<InStream, IOException> wrappedStreamOpener =
				() -> new InStream(streamOpener.get(), this);

		return createStream(wrappedStreamOpener, openInputStreams, false);
	}

	// ------------------------------------------------------------------------
	//  other delegating file system methods
	// ------------------------------------------------------------------------

	@Override
	public FileSystemKind getKind() {
		return originalFs.getKind();
	}

	@Override
	public boolean isDistributedFS() {
		return originalFs.isDistributedFS();
	}

	@Override
	public Path getWorkingDirectory() {
		return originalFs.getWorkingDirectory();
	}

	@Override
	public Path getHomeDirectory() {
		return originalFs.getHomeDirectory();
	}

	@Override
	public URI getUri() {
		return originalFs.getUri();
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		return originalFs.getFileStatus(f);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		return originalFs.getFileBlockLocations(file, start, len);
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		return originalFs.listStatus(f);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		return originalFs.delete(f, recursive);
	}

	@Override
	public boolean mkdirs(Path f) throws IOException {
		return originalFs.mkdirs(f);
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		return originalFs.rename(src, dst);
	}

	@Override
	public boolean exists(Path f) throws IOException {
		return originalFs.exists(f);
	}

	@Override
	@Deprecated
	@SuppressWarnings("deprecation")
	public long getDefaultBlockSize() {
		return originalFs.getDefaultBlockSize();
	}

	// ------------------------------------------------------------------------

	private <T extends StreamWithTimeout> T createStream(
			final SupplierWithException<T, IOException> streamOpener,
			final HashSet<T> openStreams,
			final boolean output) throws IOException {

		final int outputLimit = output && maxNumOpenOutputStreams > 0 ? maxNumOpenOutputStreams : Integer.MAX_VALUE;
		final int inputLimit = !output && maxNumOpenInputStreams > 0 ? maxNumOpenInputStreams : Integer.MAX_VALUE;
		final int totalLimit = maxNumOpenStreamsTotal > 0 ? maxNumOpenStreamsTotal : Integer.MAX_VALUE;
		final int outputCredit = output ? 1 : 0;
		final int inputCredit = output ? 0 : 1;

		// because waiting for availability may take long, we need to be interruptible here
		// and handle interrupted exceptions as I/O errors
		// even though the code is written to make sure the lock is held for a short time only,
		// making the lock acquisition interruptible helps to guard against the cases where
		// a supposedly fast operation (like 'getPos()' on a stream) actually takes long.
		try {
			lock.lockInterruptibly();
			try {
				// some integrity checks
				assert openOutputStreams.size() <= numReservedOutputStreams;
				assert openInputStreams.size() <= numReservedInputStreams;

				// wait until there are few enough streams so we can open another
				waitForAvailability(totalLimit, outputLimit, inputLimit);

				// We do not open the stream here in the locked scope because opening a stream
				// could take a while. Holding the lock during that operation would block all concurrent
				// attempts to try and open a stream, effectively serializing all calls to open the streams.
				numReservedOutputStreams += outputCredit;
				numReservedInputStreams += inputCredit;
			}
			finally {
				lock.unlock();
			}
		}
		catch (InterruptedException e) {
			// restore interruption flag
			Thread.currentThread().interrupt();
			throw new IOException("interrupted before opening stream");
		}

		// open the stream outside the lock.
		boolean success = false;
		try {
			final T out = streamOpener.get();

			// add the stream to the set, need to re-acquire the lock
			lock.lock();
			try {
				openStreams.add(out);
			} finally {
				lock.unlock();
			}

			// good, can now return cleanly
			success = true;
			return out;
		}
		finally {
			if (!success) {
				// remove the reserved credit
				// we must open this non-interruptibly, because this must succeed!
				lock.lock();
				try {
					numReservedOutputStreams -= outputCredit;
					numReservedInputStreams -= inputCredit;
					available.signalAll();
				} finally {
					lock.unlock();
				}
			}
		}
	}

	@GuardedBy("lock")
	private void waitForAvailability(
			int totalLimit,
			int outputLimit,
			int inputLimit) throws InterruptedException, IOException {

		checkState(lock.isHeldByCurrentThread());

		// compute the deadline of this operations
		final long deadline;
		if (streamOpenTimeoutNanos == 0) {
			deadline = Long.MAX_VALUE;
		} else {
			long deadlineNanos = System.nanoTime() + streamOpenTimeoutNanos;
			// check for overflow
			deadline = deadlineNanos > 0 ? deadlineNanos : Long.MAX_VALUE;
		}

		// wait for available connections
		long timeLeft;

		if (streamInactivityTimeoutNanos == 0) {
			// simple case: just wait
			while ((timeLeft = (deadline - System.nanoTime())) > 0 &&
					!hasAvailability(totalLimit, outputLimit, inputLimit)) {

				available.await(timeLeft, TimeUnit.NANOSECONDS);
			}
		}
		else {
			// complex case: chase down inactive streams
			final long checkIntervalNanos = (streamInactivityTimeoutNanos >>> 1) + 1;

			long now;
			while ((timeLeft = (deadline - (now = System.nanoTime()))) > 0 && // while still within timeout
					!hasAvailability(totalLimit, outputLimit, inputLimit)) {

				// check all streams whether there in one that has been inactive for too long
				if (!(closeInactiveStream(openOutputStreams, now) || closeInactiveStream(openInputStreams, now))) {
					// only wait if we did not manage to close any stream.
					// otherwise eagerly check again if we have availability now (we should have!)
					long timeToWait = Math.min(checkIntervalNanos, timeLeft);
					available.await(timeToWait, TimeUnit.NANOSECONDS);
				}
			}
		}

		// check for timeout
		// we check availability again to catch cases where the timeout expired while waiting
		// to re-acquire the lock
		if (timeLeft <= 0 && !hasAvailability(totalLimit, outputLimit, inputLimit)) {
			throw new IOException(String.format(
					"Timeout while waiting for an available stream/connection. " +
					"limits: total=%d, input=%d, output=%d ; Open: input=%d, output=%d ; timeout: %d ms",
					maxNumOpenStreamsTotal, maxNumOpenInputStreams, maxNumOpenOutputStreams,
					numReservedInputStreams, numReservedOutputStreams, getStreamOpenTimeout()));
		}
	}

	@GuardedBy("lock")
	private boolean hasAvailability(int totalLimit, int outputLimit, int inputLimit) {
		return numReservedOutputStreams < outputLimit &&
				numReservedInputStreams < inputLimit &&
				numReservedOutputStreams + numReservedInputStreams < totalLimit;
	}

	@GuardedBy("lock")
	private boolean closeInactiveStream(HashSet<? extends StreamWithTimeout> streams, long nowNanos) {
		for (StreamWithTimeout stream : streams) {
			try {
				final StreamProgressTracker tracker = stream.getProgressTracker();

				// If the stream is closed already, it will be removed anyways, so we
				// do not classify it as inactive. We also skip the check if another check happened too recently.
				if (stream.isClosed() || nowNanos < tracker.getLastCheckTimestampNanos() + streamInactivityTimeoutNanos) {
					// interval since last check not yet over
					return false;
				}
				else if (!tracker.checkNewBytesAndMark(nowNanos)) {
					stream.closeDueToTimeout();
					return true;
				}
			}
			catch (StreamTimeoutException ignored) {
				// may happen due to races
			}
			catch (IOException e) {
				// only log on debug level here, to avoid log spamming
				LOG.debug("Could not check for stream progress to determine inactivity", e);
			}
		}

		return false;
	}

	// ------------------------------------------------------------------------

	/**
	 * Atomically removes the given output stream from the set of currently open output streams,
	 * and signals that new stream can now be opened.
	 */
	void unregisterOutputStream(OutStream stream) {
		lock.lock();
		try {
			// only decrement if we actually remove the stream
			if (openOutputStreams.remove(stream)) {
				numReservedOutputStreams--;
				available.signalAll();
			}
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Atomically removes the given input stream from the set of currently open input streams,
	 * and signals that new stream can now be opened.
	 */
	void unregisterInputStream(InStream stream) {
		lock.lock();
		try {
			// only decrement if we actually remove the stream
			if (openInputStreams.remove(stream)) {
				numReservedInputStreams--;
				available.signalAll();
			}
		}
		finally {
			lock.unlock();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A special IOException, indicating a timeout in the data output stream.
	 */
	public static final class StreamTimeoutException extends IOException {

		private static final long serialVersionUID = -8790922066795901928L;

		public StreamTimeoutException() {
			super("Stream closed due to inactivity timeout. " +
					"This is done to prevent inactive streams from blocking the full " +
					"pool of limited connections");
		}

		public StreamTimeoutException(StreamTimeoutException other) {
			super(other.getMessage(), other);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Interface for streams that can be checked for inactivity.
	 */
	private interface StreamWithTimeout extends Closeable {

		/**
		 * Gets the progress tracker for this stream.
		 */
		StreamProgressTracker getProgressTracker();

		/**
		 * Gets the current position in the stream, as in number of bytes read or written.
		 */
		long getPos() throws IOException;

		/**
		 * Closes the stream asynchronously with a special exception that indicates closing due
		 * to lack of progress.
		 */
		void closeDueToTimeout() throws IOException;

		/**
		 * Checks whether the stream was closed already.
		 */
		boolean isClosed();
	}

	// ------------------------------------------------------------------------

	/**
	 * A tracker for stream progress. This records the number of bytes read / written together
	 * with a timestamp when the last check happened.
	 */
	private static final class StreamProgressTracker {

		/** The tracked stream. */
		private final StreamWithTimeout stream;

		/** The number of bytes written the last time that the {@link #checkNewBytesAndMark(long)}
		 * method was called. It is important to initialize this with {@code -1} so that the
		 * first check (0 bytes) always appears to have made progress. */
		private volatile long lastCheckBytes = -1;

		/** The timestamp when the last inactivity evaluation was made. */
		private volatile long lastCheckTimestampNanos;

		StreamProgressTracker(StreamWithTimeout stream) {
			this.stream = stream;
		}

		/**
		 * Gets the timestamp when the last inactivity evaluation was made.
		 */
		public long getLastCheckTimestampNanos() {
			return lastCheckTimestampNanos;
		}

		/**
		 * Checks whether there were new bytes since the last time this method was invoked.
		 * This also sets the given timestamp, to be read via {@link #getLastCheckTimestampNanos()}.
		 *
		 * @return True, if there were new bytes, false if not.
		 */
		public boolean checkNewBytesAndMark(long timestamp) throws IOException {
			// remember the time when checked
			lastCheckTimestampNanos = timestamp;

			final long bytesNow = stream.getPos();
			if (bytesNow > lastCheckBytes) {
				lastCheckBytes = bytesNow;
				return true;
			}
			else {
				return false;
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A data output stream that wraps a given data output stream and un-registers
	 * from a given connection-limiting file system
	 * (via {@link LimitedConnectionsFileSystem#unregisterOutputStream(OutStream)}
	 * upon closing.
	 */
	private static final class OutStream extends FSDataOutputStream implements StreamWithTimeout {

		/** The original data output stream to write to. */
		private final FSDataOutputStream originalStream;

		/** The connection-limiting file system to un-register from. */
		private final LimitedConnectionsFileSystem fs;

		/** The progress tracker for this stream. */
		private final StreamProgressTracker progressTracker;

		/** An exception with which the stream has been externally closed. */
		private volatile StreamTimeoutException timeoutException;

		/** Flag tracking whether the stream was already closed, for proper inactivity tracking. */
		private final AtomicBoolean closed = new AtomicBoolean();

		OutStream(FSDataOutputStream originalStream, LimitedConnectionsFileSystem fs) {
			this.originalStream = checkNotNull(originalStream);
			this.fs = checkNotNull(fs);
			this.progressTracker = new StreamProgressTracker(this);
		}

		// --- FSDataOutputStream API implementation

		@Override
		public void write(int b) throws IOException {
			try {
				originalStream.write(b);
			}
			catch (IOException e) {
				handleIOException(e);
			}
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			try {
				originalStream.write(b, off, len);
			}
			catch (IOException e) {
				handleIOException(e);
			}
		}

		@Override
		public long getPos() throws IOException {
			try {
				return originalStream.getPos();
			}
			catch (IOException e) {
				handleIOException(e);
				return -1; // silence the compiler
			}
		}

		@Override
		public void flush() throws IOException {
			try {
				originalStream.flush();
			}
			catch (IOException e) {
				handleIOException(e);
			}
		}

		@Override
		public void sync() throws IOException {
			try {
				originalStream.sync();
			}
			catch (IOException e) {
				handleIOException(e);
			}
		}

		@Override
		public void close() throws IOException {
			if (closed.compareAndSet(false, true)) {
				try {
					originalStream.close();
				}
				catch (IOException e) {
					handleIOException(e);
				}
				finally {
					fs.unregisterOutputStream(this);
				}
			}
		}

		@Override
		public void closeDueToTimeout() throws IOException {
			this.timeoutException = new StreamTimeoutException();
			close();
		}

		@Override
		public boolean isClosed() {
			return closed.get();
		}

		@Override
		public StreamProgressTracker getProgressTracker() {
			return progressTracker;
		}

		private void handleIOException(IOException exception) throws IOException {
			if (timeoutException == null) {
				throw exception;
			} else {
				// throw a new exception to capture this call's stack trace
				// the new exception is forwarded as a suppressed exception
				StreamTimeoutException te = new StreamTimeoutException(timeoutException);
				te.addSuppressed(exception);
				throw te;
			}
		}
	}

	/**
	 * A data input stream that wraps a given data input stream and un-registers
	 * from a given connection-limiting file system
	 * (via {@link LimitedConnectionsFileSystem#unregisterInputStream(InStream)}
	 * upon closing.
	 */
	private static final class InStream extends FSDataInputStream implements StreamWithTimeout {

		/** The original data input stream to read from. */
		private final FSDataInputStream originalStream;

		/** The connection-limiting file system to un-register from. */
		private final LimitedConnectionsFileSystem fs;

		/** An exception with which the stream has been externally closed. */
		private volatile StreamTimeoutException timeoutException;

		/** The progress tracker for this stream. */
		private final StreamProgressTracker progressTracker;

		/** Flag tracking whether the stream was already closed, for proper inactivity tracking. */
		private final AtomicBoolean closed = new AtomicBoolean();

		InStream(FSDataInputStream originalStream, LimitedConnectionsFileSystem fs) {
			this.originalStream = checkNotNull(originalStream);
			this.fs = checkNotNull(fs);
			this.progressTracker = new StreamProgressTracker(this);
		}

		// --- FSDataOutputStream API implementation

		@Override
		public int read() throws IOException {
			try {
				return originalStream.read();
			}
			catch (IOException e) {
				handleIOException(e);
				return 0; // silence the compiler
			}
		}

		@Override
		public int read(byte[] b) throws IOException {
			try {
				return originalStream.read(b);
			}
			catch (IOException e) {
				handleIOException(e);
				return 0; // silence the compiler
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			try {
				return originalStream.read(b, off, len);
			}
			catch (IOException e) {
				handleIOException(e);
				return 0; // silence the compiler
			}
		}

		@Override
		public long skip(long n) throws IOException {
			try {
				return originalStream.skip(n);
			}
			catch (IOException e) {
				handleIOException(e);
				return 0L; // silence the compiler
			}
		}

		@Override
		public int available() throws IOException {
			try {
				return originalStream.available();
			}
			catch (IOException e) {
				handleIOException(e);
				return 0; // silence the compiler
			}
		}

		@Override
		public void mark(int readlimit) {
			originalStream.mark(readlimit);
		}

		@Override
		public void reset() throws IOException {
			try {
				originalStream.reset();
			}
			catch (IOException e) {
				handleIOException(e);
			}
		}

		@Override
		public boolean markSupported() {
			return originalStream.markSupported();
		}

		@Override
		public void seek(long desired) throws IOException {
			try {
				originalStream.seek(desired);
			}
			catch (IOException e) {
				handleIOException(e);
			}
		}

		@Override
		public long getPos() throws IOException {
			try {
				return originalStream.getPos();
			}
			catch (IOException e) {
				handleIOException(e);
				return 0; // silence the compiler
			}
		}

		@Override
		public void close() throws IOException {
			if (closed.compareAndSet(false, true)) {
				try {
					originalStream.close();
				}
				catch (IOException e) {
					handleIOException(e);
				}
				finally {
					fs.unregisterInputStream(this);
				}
			}
		}

		@Override
		public void closeDueToTimeout() throws IOException {
			this.timeoutException = new StreamTimeoutException();
			close();
		}

		@Override
		public boolean isClosed() {
			return closed.get();
		}

		@Override
		public StreamProgressTracker getProgressTracker() {
			return progressTracker;
		}

		private void handleIOException(IOException exception) throws IOException {
			if (timeoutException == null) {
				throw exception;
			} else {
				// throw a new exception to capture this call's stack trace
				// the new exception is forwarded as a suppressed exception
				StreamTimeoutException te = new StreamTimeoutException(timeoutException);
				te.addSuppressed(exception);
				throw te;
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A simple configuration data object capturing the settings for limited connections.
	 */
	public static class ConnectionLimitingSettings {

		/** The limit for the total number of connections, or 0, if no limit. */
		public final int limitTotal;

		/** The limit for the number of input stream connections, or 0, if no limit. */
		public final int limitInput;

		/** The limit for the number of output stream connections, or 0, if no limit. */
		public final int limitOutput;

		/** The stream opening timeout for a stream, in milliseconds. */
		public final long streamOpenTimeout;

		/** The inactivity timeout for a stream, in milliseconds. */
		public final long streamInactivityTimeout;

		/**
		 * Creates a new ConnectionLimitingSettings with the given parameters.
		 *
		 * @param limitTotal The limit for the total number of connections, or 0, if no limit.
		 * @param limitInput The limit for the number of input stream connections, or 0, if no limit.
		 * @param limitOutput The limit for the number of output stream connections, or 0, if no limit.
		 * @param streamOpenTimeout       The maximum number of milliseconds that the file system will wait when
		 *                                no more connections are currently permitted.
		 * @param streamInactivityTimeout The milliseconds that a stream may spend not writing any
		 *                                bytes before it is closed as inactive.
		 */
		public ConnectionLimitingSettings(
				int limitTotal,
				int limitInput,
				int limitOutput,
				long streamOpenTimeout,
				long streamInactivityTimeout) {
			checkArgument(limitTotal >= 0);
			checkArgument(limitInput >= 0);
			checkArgument(limitOutput >= 0);
			checkArgument(streamOpenTimeout >= 0);
			checkArgument(streamInactivityTimeout >= 0);

			this.limitTotal = limitTotal;
			this.limitInput = limitInput;
			this.limitOutput = limitOutput;
			this.streamOpenTimeout = streamOpenTimeout;
			this.streamInactivityTimeout = streamInactivityTimeout;
		}

		// --------------------------------------------------------------------

		/**
		 * Parses and returns the settings for connection limiting, for the file system with
		 * the given file system scheme.
		 *
		 * @param config The configuration to check.
		 * @param fsScheme The file system scheme.
		 *
		 * @return The parsed configuration, or null, if no connection limiting is configured.
		 */
		@Nullable
		public static ConnectionLimitingSettings fromConfig(Configuration config, String fsScheme) {
			checkNotNull(fsScheme, "fsScheme");
			checkNotNull(config, "config");

			final ConfigOption<Integer> totalLimitOption = CoreOptions.fileSystemConnectionLimit(fsScheme);
			final ConfigOption<Integer> limitInOption = CoreOptions.fileSystemConnectionLimitIn(fsScheme);
			final ConfigOption<Integer> limitOutOption = CoreOptions.fileSystemConnectionLimitOut(fsScheme);

			final int totalLimit = config.getInteger(totalLimitOption);
			final int limitIn = config.getInteger(limitInOption);
			final int limitOut = config.getInteger(limitOutOption);

			checkLimit(totalLimit, totalLimitOption);
			checkLimit(limitIn, limitInOption);
			checkLimit(limitOut, limitOutOption);

			// create the settings only, if at least one limit is configured
			if (totalLimit <= 0 && limitIn <= 0 && limitOut <= 0) {
				// no limit configured
				return null;
			}
			else {
				final ConfigOption<Long> openTimeoutOption =
						CoreOptions.fileSystemConnectionLimitTimeout(fsScheme);
				final ConfigOption<Long> inactivityTimeoutOption =
						CoreOptions.fileSystemConnectionLimitStreamInactivityTimeout(fsScheme);

				final long openTimeout = config.getLong(openTimeoutOption);
				final long inactivityTimeout = config.getLong(inactivityTimeoutOption);

				checkTimeout(openTimeout, openTimeoutOption);
				checkTimeout(inactivityTimeout, inactivityTimeoutOption);

				return new ConnectionLimitingSettings(
						totalLimit == -1 ? 0 : totalLimit,
						limitIn == -1 ? 0 : limitIn,
						limitOut == -1 ? 0 : limitOut,
						openTimeout,
						inactivityTimeout);
			}
		}

		private static void checkLimit(int value, ConfigOption<Integer> option) {
			if (value < -1) {
				throw new IllegalConfigurationException("Invalid value for '" + option.key() + "': " + value);
			}
		}

		private static void checkTimeout(long timeout, ConfigOption<Long> option) {
			if (timeout < 0) {
				throw new IllegalConfigurationException("Invalid value for '" + option.key() + "': " + timeout);
			}
		}
	}
}
