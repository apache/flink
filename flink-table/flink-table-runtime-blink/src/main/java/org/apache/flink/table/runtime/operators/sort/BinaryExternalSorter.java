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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExceptionHandler;
import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link BinaryExternalSorter} is a full fledged sorter for binary format.
 * It implements a multi-way merge sort.
 * Internally, it has three asynchronous threads (sort, spill, merger) which communicate through
 * a set of blocking circularQueues, forming a closed loop. Memory is allocated using the
 * {@link MemoryManager} interface. Thus the component will not exceed the provided memory limits.
 */
public class BinaryExternalSorter implements Sorter<BinaryRowData> {

	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	/** The minimum number of segments that are required for the sort to operate. */
	private static final int MIN_NUM_SORT_MEM_SEGMENTS = 10;

	private static final long SORTER_MIN_NUM_SORT_MEM =
			MIN_NUM_SORT_MEM_SEGMENTS * MemoryManager.DEFAULT_PAGE_SIZE;

	/** Logging. */
	private static final Logger LOG = LoggerFactory.getLogger(BinaryExternalSorter.class);

	// ------------------------------------------------------------------------
	//                                  Threads
	// ------------------------------------------------------------------------

	/**
	 * The currWriteBuffer that is passed as marker for the end of data.
	 */
	private static final CircularElement EOF_MARKER = new CircularElement();

	/**
	 * The currWriteBuffer that is passed as marker for signaling the beginning of spilling.
	 */
	private static final CircularElement SPILLING_MARKER = new CircularElement();

	/**
	 * The ChannelWithMeta that is passed as marker for signaling the final merge.
	 */
	private static final ChannelWithMeta FINAL_MERGE_MARKER = new ChannelWithMeta(null, -1, -1);

	// ------------------------------------------------------------------------
	//                                   Memory
	// ------------------------------------------------------------------------

	/**
	 * The memory segments used first for sorting and later for reading/pre-fetching
	 * during the external merge.
	 */
	private final List<LazyMemorySegmentPool> sortReadMemory;

	/**
	 * Records all sort buffer.
	 */
	private final List<BinaryInMemorySortBuffer> sortBuffers;

	// ------------------------------------------------------------------------
	//                            Miscellaneous Fields
	// ------------------------------------------------------------------------

	/**
	 * The monitor which guards the iterator field.
	 */
	private final Object iteratorLock = new Object();

	/** The thread that merges the buffer handed from the reading thread. */
	private ThreadBase sortThread;

	/** The thread that handles spilling to secondary storage. */
	private ThreadBase spillThread;

	/** The thread that handles merging from the secondary storage. */
	private ThreadBase mergeThread;

	/**
	 * Final result iterator.
	 */
	private volatile MutableObjectIterator<BinaryRowData> iterator;

	/**
	 * The exception that is set, if the iterator cannot be created.
	 */
	private volatile IOException iteratorException;

	/**
	 * Flag indicating that the sorter was closed.
	 */
	private volatile boolean closed;

	/**
	 * Sort or spill thread maybe occur some exceptions.
	 */
	private ExceptionHandler<IOException> exceptionHandler;

	/**
	 * Queue for the communication between the threads.
	 */
	private CircularQueues circularQueues;

	private long bytesUntilSpilling;

	private CircularElement currWriteBuffer;

	private boolean writingDone = false;

	private final Object writeLock = new Object();

	private final SpillChannelManager channelManager;

	private final BinaryExternalMerger merger;

	private final int memorySegmentSize;

	private final boolean compressionEnable;
	private final BlockCompressionFactory compressionCodecFactory;
	private final int compressionBlockSize;

	private final boolean asyncMergeEnable;

	// ------------------------------------------------------------------------
	//                         Constructor & Shutdown
	// ------------------------------------------------------------------------

	private final BinaryRowDataSerializer serializer;

	//metric
	private long numSpillFiles;
	private long spillInBytes;
	private long spillInCompressedBytes;

	public BinaryExternalSorter(
			final Object owner, MemoryManager memoryManager, long reservedMemorySize,
			IOManager ioManager, AbstractRowDataSerializer<RowData> inputSerializer,
			BinaryRowDataSerializer serializer, NormalizedKeyComputer normalizedKeyComputer,
			RecordComparator comparator, Configuration conf) {
		this(owner, memoryManager, reservedMemorySize, ioManager,
				inputSerializer, serializer, normalizedKeyComputer, comparator,
				conf, AlgorithmOptions.SORT_SPILLING_THRESHOLD.defaultValue());
	}

	public BinaryExternalSorter(
			final Object owner, MemoryManager memoryManager, long reservedMemorySize,
			IOManager ioManager, AbstractRowDataSerializer<RowData> inputSerializer,
			BinaryRowDataSerializer serializer,
			NormalizedKeyComputer normalizedKeyComputer,
			RecordComparator comparator, Configuration conf,
			float startSpillingFraction) {
		int maxNumFileHandles = conf.getInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES);
		this.compressionEnable = conf.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED);
		this.compressionCodecFactory = this.compressionEnable
			? BlockCompressionFactory.createBlockCompressionFactory(
					BlockCompressionFactory.CompressionFactoryName.LZ4.toString())
			: null;
		this.compressionBlockSize = (int) MemorySize.parse(
			conf.getString(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)).getBytes();
		asyncMergeEnable = conf.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED);

		checkArgument(maxNumFileHandles >= 2);
		checkNotNull(ioManager);
		checkNotNull(normalizedKeyComputer);
		checkNotNull(memoryManager);
		this.serializer = (BinaryRowDataSerializer) serializer.duplicate();
		this.memorySegmentSize = memoryManager.getPageSize();

		if (reservedMemorySize < SORTER_MIN_NUM_SORT_MEM) {
			throw new IllegalArgumentException("Too little memory provided to sorter to perform task. " +
					"Required are at least " + SORTER_MIN_NUM_SORT_MEM +
					" bytes. Current memory size is " + reservedMemorySize + " bytes.");
		}

		// adjust the memory quotas to the page size
		final int sortMemPages = (int) (reservedMemorySize / memoryManager.getPageSize());
		final long sortMemory = ((long) sortMemPages) * memoryManager.getPageSize();

		// decide how many sort buffers to use
		int numSortBuffers = 1;
		if (reservedMemorySize > 100 * 1024 * 1024L) {
			numSortBuffers = 2;
		}
		final int numSegmentsPerSortBuffer = sortMemPages / numSortBuffers;
		this.sortReadMemory = new ArrayList<>();

		// circular circularQueues pass buffers between the threads
		final CircularQueues circularQueues = new CircularQueues();

		LOG.info("BinaryExternalSorter with initial memory segments {}, " +
				"maxNumFileHandles({}), compressionEnable({}), compressionCodecFactory({}), compressionBlockSize({}).",
				sortMemPages, maxNumFileHandles, compressionEnable,
				compressionEnable ? compressionCodecFactory.getClass() : null, compressionBlockSize);

		this.sortBuffers = new ArrayList<>();
		int totalBuffers = sortMemPages;
		for (int i = 0; i < numSortBuffers; i++) {
			// grab some memory
			int sortSegments = Math.min(i == numSortBuffers - 1 ? Integer.MAX_VALUE : numSegmentsPerSortBuffer, totalBuffers);
			totalBuffers -= sortSegments;
			LazyMemorySegmentPool pool = new LazyMemorySegmentPool(owner, memoryManager, sortSegments);
			this.sortReadMemory.add(pool);
			final BinaryInMemorySortBuffer buffer = BinaryInMemorySortBuffer.createBuffer(
					normalizedKeyComputer, inputSerializer, serializer, comparator, pool);

			// add to empty queue
			CircularElement element = new CircularElement(i, buffer);
			circularQueues.empty.add(element);
			this.sortBuffers.add(buffer);
		}

		// exception handling
		ExceptionHandler<IOException> exceptionHandler = exception -> {
			// forward exception
			if (!closed) {
				setResultIteratorException(exception);
				close();
			}
		};

		// init adding currWriteBuffer
		this.exceptionHandler = exceptionHandler;
		this.circularQueues = circularQueues;

		bytesUntilSpilling = ((long) (startSpillingFraction * sortMemory));

		// check if we should directly spill
		if (bytesUntilSpilling < 1) {
			bytesUntilSpilling = 0;
			// add the spilling marker
			this.circularQueues.sort.add(SPILLING_MARKER);
		}

		this.channelManager = new SpillChannelManager();
		this.merger = new BinaryExternalMerger(
				ioManager, memoryManager.getPageSize(),
				maxNumFileHandles, channelManager,
				(BinaryRowDataSerializer) serializer.duplicate(), comparator,
				compressionEnable, compressionCodecFactory, compressionBlockSize);

		// start the thread that sorts the buffers
		this.sortThread = getSortingThread(exceptionHandler, circularQueues);

		// start the thread that handles spilling to secondary storage
		this.spillThread = getSpillingThread(
				exceptionHandler, circularQueues, ioManager,
				(BinaryRowDataSerializer) serializer.duplicate(), comparator);

		// start the thread that handles merging from second storage
		this.mergeThread = getMergingThread(
				exceptionHandler, circularQueues, maxNumFileHandles, merger);

		// propagate the context class loader to the spawned threads
		ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
		if (contextLoader != null) {
			if (this.sortThread != null) {
				this.sortThread.setContextClassLoader(contextLoader);
			}
			if (this.spillThread != null) {
				this.spillThread.setContextClassLoader(contextLoader);
			}
			if (this.mergeThread != null) {
				this.mergeThread.setContextClassLoader(contextLoader);
			}
		}
	}

	// ------------------------------------------------------------------------
	//                           Factory Methods
	// ------------------------------------------------------------------------

	/**
	 * Starts all the threads that are used by this sorter.
	 */
	public void startThreads() {
		if (this.sortThread != null) {
			this.sortThread.start();
		}
		if (this.spillThread != null) {
			this.spillThread.start();
		}
		if (this.mergeThread != null) {
			this.mergeThread.start();
		}
	}

	/**
	 * Shuts down all the threads initiated by this sorter. Also releases all previously allocated
	 * memory, if it has not yet been released by the threads, and closes and deletes all channels
	 * (removing the temporary files).
	 *
	 * <p>The threads are set to exit directly, but depending on their operation, it may take a
	 * while to actually happen. The sorting thread will for example not finish before the current
	 * batch is sorted. This method attempts to wait for the working thread to exit. If it is
	 * however interrupted, the method exits immediately and is not guaranteed how long the threads
	 * continue to exist and occupy resources afterwards.
	 */
	@Override
	public void close() {
		// check if the sorter has been closed before
		synchronized (this) {
			if (this.closed) {
				return;
			}

			// mark as closed
			this.closed = true;
		}

		// from here on, the code is in a try block, because even through errors might be thrown in this block,
		// we need to make sure that all the memory is released.
		try {
			// if the result iterator has not been obtained yet, set the exception
			synchronized (this.iteratorLock) {
				if (this.iteratorException == null) {
					this.iteratorException = new IOException("The sorter has been closed.");
					this.iteratorLock.notifyAll();
				}
			}

			// stop all the threads
			if (this.sortThread != null) {
				try {
					this.sortThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down sorter thread: " + t.getMessage(), t);
				}
			}
			if (this.spillThread != null) {
				try {
					this.spillThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down spilling thread: " + t.getMessage(), t);
				}
			}
			if (this.mergeThread != null) {
				try {
					this.mergeThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down merging thread: " + t.getMessage(), t);
				}
			}

			try {
				if (this.sortThread != null) {
					this.sortThread.join();
					this.sortThread = null;
				}
				if (this.spillThread != null) {
					this.spillThread.join();
					this.spillThread = null;
				}
				if (this.mergeThread != null) {
					this.mergeThread.join();
					this.mergeThread = null;
				}
			} catch (InterruptedException iex) {
				LOG.debug("Closing of sort/merger was interrupted. " +
						"The reading/sorting/spilling/merging threads may still be working.", iex);
			}
		} finally {
			releaseSortMemory();

			// Eliminate object references for MemorySegments.
			circularQueues = null;
			currWriteBuffer = null;
			iterator = null;

			merger.close();
			channelManager.close();
		}
	}

	private void releaseSortMemory() {
		// RELEASE ALL MEMORY. If the threads and channels are still running, this should cause
		// exceptions, because their memory segments are freed

		try {
			// floating segments are released in `dispose()` method
			this.sortBuffers.forEach(BinaryInMemorySortBuffer::dispose);
			this.sortBuffers.clear();
		} catch (Throwable e) {
			LOG.info("error.", e);
		}

		sortReadMemory.forEach(LazyMemorySegmentPool::close);
		sortReadMemory.clear();
	}

	private ThreadBase getSortingThread(ExceptionHandler<IOException> exceptionHandler,
			CircularQueues queues) {
		return new SortingThread(exceptionHandler, queues);
	}

	private SpillingThread getSpillingThread(ExceptionHandler<IOException> exceptionHandler,
			CircularQueues queues, IOManager ioManager,
			BinaryRowDataSerializer serializer, RecordComparator comparator) {
		return new SpillingThread(exceptionHandler, queues, ioManager, serializer, comparator);
	}

	private MergingThread getMergingThread(
			ExceptionHandler<IOException> exceptionHandler,
			CircularQueues queues,
			int maxNumFileHandles,
			BinaryExternalMerger merger) {
		return new MergingThread(exceptionHandler, queues, maxNumFileHandles, merger);
	}

	public void write(RowData current) throws IOException {
		checkArgument(!writingDone, "Adding already done!");
		try {
			while (true) {
				if (closed) {
					throw new IOException("Already closed!", iteratorException);
				}

				synchronized (writeLock) {
					// grab the next buffer
					if (currWriteBuffer == null) {
						try {
							currWriteBuffer = this.circularQueues.empty.poll(1, TimeUnit.SECONDS);
							if (currWriteBuffer == null) {
								// maybe something happened, release lock.
								continue;
							}
							if (!currWriteBuffer.buffer.isEmpty()) {
								throw new IOException("New buffer is not empty.");
							}
						} catch (InterruptedException iex) {
							throw new IOException(iex);
						}
					}

					final BinaryInMemorySortBuffer buffer = currWriteBuffer.buffer;

					if (LOG.isDebugEnabled()) {
						LOG.debug("Retrieved empty read buffer " + currWriteBuffer.id + ".");
					}

					long occupancy = buffer.getOccupancy();
					if (!buffer.write(current)) {
						if (buffer.isEmpty()) {
							// did not fit in a fresh buffer, must be large...
							throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
									+ buffer.getCapacity() + " bytes).");
						} else {
							// buffer is full, send the buffer
							if (LOG.isDebugEnabled()) {
								LOG.debug("Emitting full buffer: " + currWriteBuffer.id + ".");
							}

							this.circularQueues.sort.add(currWriteBuffer);

							// Deadlocks may occur when there are fewer MemorySegments, because of
							// the fragmentation of buffer.getOccupancy ().
							if (bytesUntilSpilling > 0 && circularQueues.empty.size() == 0) {
								bytesUntilSpilling = 0;
								this.circularQueues.sort.add(SPILLING_MARKER);
							}

							currWriteBuffer = null;
							// continue to process current record.
						}
					} else {
						// successfully added record
						// it may be that the last currWriteBuffer would have crossed the
						// spilling threshold, so check it
						if (bytesUntilSpilling > 0) {
							bytesUntilSpilling -= buffer.getOccupancy() - occupancy;
							if (bytesUntilSpilling <= 0) {
								bytesUntilSpilling = 0;
								this.circularQueues.sort.add(SPILLING_MARKER);
							}
						}
						break;
					}
				}
			}
		} catch (Throwable e) {
			IOException ioe = new IOException(e);
			if (this.exceptionHandler != null) {
				this.exceptionHandler.handleException(ioe);
			}
			throw ioe;
		}
	}

	@VisibleForTesting
	public void write(MutableObjectIterator<BinaryRowData> iterator) throws IOException {
		BinaryRowData row = serializer.createInstance();
		while ((row = iterator.next(row)) != null) {
			write(row);
		}
	}

	@Override
	public MutableObjectIterator<BinaryRowData> getIterator() throws InterruptedException {
		if (!writingDone) {
			writingDone = true;

			if (currWriteBuffer != null) {
				this.circularQueues.sort.add(currWriteBuffer);
			}

			// add the sentinel to notify the receivers that the work is done
			// send the EOF marker
			this.circularQueues.sort.add(EOF_MARKER);
			LOG.debug("Sending done.");
		}

		synchronized (this.iteratorLock) {
			// wait while both the iterator and the exception are not set
			while (this.iterator == null && this.iteratorException == null) {
				this.iteratorLock.wait();
			}

			if (this.iteratorException != null) {
				throw new RuntimeException("Error obtaining the sorted input: " + this.iteratorException.getMessage(),
						this.iteratorException);
			} else {
				return this.iterator;
			}
		}
	}

	// ------------------------------------------------------------------------
	// Inter-Thread Communication
	// ------------------------------------------------------------------------

	/**
	 * Sets the result iterator. By setting the result iterator, all threads that are waiting for
	 * the result
	 * iterator are notified and will obtain it.
	 *
	 * @param iterator The result iterator to set.
	 */
	private void setResultIterator(MutableObjectIterator<BinaryRowData> iterator) {
		synchronized (this.iteratorLock) {
			// set the result iterator only, if no exception has occurred
			if (this.iteratorException == null) {
				this.iterator = iterator;
				this.iteratorLock.notifyAll();
			}
		}
	}

	/**
	 * Reports an exception to all threads that are waiting for the result iterator.
	 *
	 * @param ioex The exception to be reported to the threads that wait for the result iterator.
	 */
	private void setResultIteratorException(IOException ioex) {
		synchronized (this.iteratorLock) {
			if (this.iteratorException == null) {
				this.iteratorException = ioex;
				this.iteratorLock.notifyAll();
			}
		}
	}

	/**
	 * Class representing buffers that circulate between the reading, sorting and spilling thread.
	 */
	private static final class CircularElement {

		final int id; // just for debug.
		final BinaryInMemorySortBuffer buffer;

		private CircularElement() {
			this.id = -1;
			this.buffer = null;
		}

		private CircularElement(int id, BinaryInMemorySortBuffer buffer) {
			this.id = id;
			this.buffer = buffer;
		}
	}

	/**
	 * Collection of circularQueues that are used for the communication between the threads.
	 */
	private static final class CircularQueues {

		final BlockingQueue<CircularElement> empty;

		final BlockingQueue<CircularElement> sort;

		final BlockingQueue<CircularElement> spill;

		final BlockingQueue<ChannelWithMeta> merge;

		private CircularQueues() {
			this.empty = new LinkedBlockingQueue<>();
			this.sort = new LinkedBlockingQueue<>();
			this.spill = new LinkedBlockingQueue<>();
			this.merge = new LinkedBlockingQueue<>();
		}
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	/**
	 * Base class for all working threads in this sorter. The specific threads for sorting,
	 * spilling, etc... extend this subclass.
	 *
	 * <p>The threads are designed to terminate themselves when the task they are set up to do is
	 * completed. Further more, they terminate immediately when the <code>shutdown()</code> method
	 * is called.
	 */
	private abstract static class ThreadBase extends Thread implements Thread.UncaughtExceptionHandler {

		/**
		 * The queue of empty buffer that can be used for reading.
		 */
		final CircularQueues queues;

		/**
		 * The exception handler for any problems.
		 */
		private final ExceptionHandler<IOException> exceptionHandler;

		/**
		 * The flag marking this thread as alive.
		 */
		private volatile boolean alive;

		/**
		 * Creates a new thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param name             The name of the thread.
		 * @param queues           The circularQueues used to pass buffers between the threads.
		 */
		private ThreadBase(ExceptionHandler<IOException> exceptionHandler, String name,
				CircularQueues queues) {
			// thread setup
			super(name);
			this.setDaemon(true);

			// exception handling
			this.exceptionHandler = exceptionHandler;
			this.setUncaughtExceptionHandler(this);

			this.queues = queues;
			this.alive = true;
		}

		/**
		 * Implements exception handling and delegates to go().
		 */
		public void run() {
			try {
				go();
			} catch (Throwable t) {
				internalHandleException(new IOException("Thread '" + getName() + "' terminated due to an exception: "
						+ t.getMessage(), t));
			}
		}

		/**
		 * Equivalent to the run() method.
		 *
		 * @throws IOException Exceptions that prohibit correct completion of the work may be thrown
		 *                     by the thread.
		 */
		abstract void go() throws IOException;

		/**
		 * Checks whether this thread is still alive.
		 *
		 * @return true, if the thread is alive, false otherwise.
		 */
		public boolean isRunning() {
			return this.alive;
		}

		/**
		 * Forces an immediate shutdown of the thread. Looses any state and all buffers that the
		 * thread is currently
		 * working on. This terminates cleanly for the JVM, but looses intermediate results.
		 */
		public void shutdown() {
			this.alive = false;
			this.interrupt();
		}

		/**
		 * Internally handles an exception and makes sure that this method returns without a
		 * problem.
		 *
		 * @param ioex The exception to handle.
		 */
		private void internalHandleException(IOException ioex) {
			if (!isRunning()) {
				// discard any exception that occurs when after the thread is killed.
				return;
			}
			if (this.exceptionHandler != null) {
				try {
					this.exceptionHandler.handleException(ioex);
				} catch (Throwable ignored) {
				}
			}
		}

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			internalHandleException(new IOException("Thread '" + t.getName()
					+ "' terminated due to an uncaught exception: " + e.getMessage(), e));
		}
	}

	/**
	 * The thread that sorts filled buffers.
	 */
	private static class SortingThread extends ThreadBase {

		private final IndexedSorter sorter;

		/**
		 * Creates a new sorting thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param queues           The circularQueues used to pass buffers between the threads.
		 */
		public SortingThread(ExceptionHandler<IOException> exceptionHandler,
				CircularQueues queues) {
			super(exceptionHandler, "SortMerger sorting thread", queues);

			// members
			this.sorter = new QuickSort();
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {
			boolean alive = true;

			// loop as long as the thread is marked alive
			while (isRunning() && alive) {
				CircularElement element;
				try {
					element = this.queues.sort.take();
				} catch (InterruptedException iex) {
					if (isRunning()) {
						if (LOG.isErrorEnabled()) {
							LOG.error(
									"Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
											"Retrying to grab buffer...");
						}
						continue;
					} else {
						return;
					}
				}

				if (element != EOF_MARKER && element != SPILLING_MARKER) {

					if (element.buffer.size() == 0) {
						element.buffer.reset();
						this.queues.empty.add(element);
						continue;
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorting buffer " + element.id + ".");
					}

					this.sorter.sort(element.buffer);

					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorted buffer " + element.id + ".");
					}
				} else if (element == EOF_MARKER) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorting thread done.");
					}
					alive = false;
				}
				this.queues.spill.add(element);
			}
		}
	}

	/**
	 * The thread that handles the spilling of intermediate results.
	 */
	private class SpillingThread extends ThreadBase {

		private final IOManager ioManager;                // I/O manager to create channels

		private final BinaryRowDataSerializer serializer;     // The serializer for the data type

		private final RecordComparator comparator;

		/**
		 * Creates the spilling thread.
		 * @param exceptionHandler  The exception handler to call for all exceptions.
		 * @param queues            The circularQueues used to pass buffers between the threads.
		 * @param ioManager         The I/O manager used to instantiate readers and writers from.
		 * @param serializer
		 * @param comparator
		 */
		public SpillingThread(ExceptionHandler<IOException> exceptionHandler,
				CircularQueues queues, IOManager ioManager,
				BinaryRowDataSerializer serializer, RecordComparator comparator) {
			super(exceptionHandler, "SortMerger spilling thread", queues);
			this.ioManager = ioManager;
			this.serializer = serializer;
			this.comparator = comparator;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {

			final Queue<CircularElement> cache = new ArrayDeque<>();
			CircularElement element;
			boolean cacheOnly = false;

			// ------------------- In-Memory Cache ------------------------
			// fill cache
			while (isRunning()) {
				// take next currWriteBuffer from queue
				try {
					element = this.queues.spill.take();
				} catch (InterruptedException iex) {
					throw new IOException("The spilling thread was interrupted.");
				}

				if (element == SPILLING_MARKER) {
					break;
				} else if (element == EOF_MARKER) {
					cacheOnly = true;
					break;
				}
				cache.add(element);
			}

			// check whether the thread was canceled
			if (!isRunning()) {
				return;
			}

			// ------------------- In-Memory Merge ------------------------
			if (cacheOnly) {
				List<MutableObjectIterator<BinaryRowData>> iterators = new ArrayList<>(cache.size());

				for (CircularElement cached : cache) {
					iterators.add(cached.buffer.getIterator());
				}

				// set lazy iterator
				List<BinaryRowData> reusableEntries = new ArrayList<>();
				for (int i = 0; i < iterators.size(); i++) {
					reusableEntries.add(serializer.createInstance());
				}
				setResultIterator(iterators.isEmpty() ? EmptyMutableObjectIterator.get() :
						iterators.size() == 1 ? iterators.get(0) : new BinaryMergeIterator<>(
								iterators, reusableEntries, comparator::compare));

				releaseEmptyBuffers();

				// signal merging thread to exit (because there is nothing to merge externally)
				this.queues.merge.add(FINAL_MERGE_MARKER);

				return;
			}

			// ------------------- Spilling Phase ------------------------

			final FileIOChannel.Enumerator enumerator =
					this.ioManager.createChannelEnumerator();

			// loop as long as the thread is marked alive and we do not see the final currWriteBuffer
			while (isRunning()) {
				try {
					element = cache.isEmpty() ? queues.spill.take() : cache.poll();
				} catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Spilling thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
						continue;
					} else {
						return;
					}
				}

				// check if we are still running
				if (!isRunning()) {
					return;
				}
				// check if this is the end-of-work buffer
				if (element == EOF_MARKER) {
					break;
				}

				if (element.buffer.getOccupancy() > 0) {
					// open next channel
					FileIOChannel.ID channel = enumerator.next();
					channelManager.addChannel(channel);

					AbstractChannelWriterOutputView output = null;
					int bytesInLastBuffer;
					int blockCount;

					try {
						numSpillFiles++;
						output = FileChannelUtil.createOutputView(ioManager, channel, compressionEnable,
								compressionCodecFactory, compressionBlockSize, memorySegmentSize);
						element.buffer.writeToOutput(output);
						spillInBytes += output.getNumBytes();
						spillInCompressedBytes += output.getNumCompressedBytes();
						bytesInLastBuffer = output.close();
						blockCount = output.getBlockCount();
						LOG.info("here spill the {}th sort buffer data with {} bytes and {} compressed bytes",
								numSpillFiles, spillInBytes, spillInCompressedBytes);
					} catch (IOException e) {
						if (output != null) {
							output.close();
							output.getChannel().deleteChannel();
						}
						throw e;
					}

					// pass spill file meta to merging thread
					this.queues.merge.add(new ChannelWithMeta(channel, blockCount, bytesInLastBuffer));
				}

				// pass empty sort-buffer to reading thread
				element.buffer.reset();
				this.queues.empty.add(element);
			}

			// clear the sort buffers, as both sorting and spilling threads are done.
			releaseSortMemory();

			// signal merging thread to begin the final merge
			this.queues.merge.add(FINAL_MERGE_MARKER);

			// Spilling thread done.
		}

		private void releaseEmptyBuffers() {
			while (!this.queues.empty.isEmpty()) {
				try {
					CircularElement element = this.queues.empty.take();
					element.buffer.dispose();
				} catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Spilling thread was interrupted (without being shut down) while collecting empty " +
							"buffers to release them. Retrying to collect buffers...");
					} else {
						break;
					}
				}
			}
			sortReadMemory.forEach(LazyMemorySegmentPool::cleanCache);
		}
	}

	/**
	 * The thread that merges the intermediate spill files and the merged files
	 * until sufficiently few channels remain to perform the final streamed merge.
	 */
	private class MergingThread extends ThreadBase {

		private final int maxFanIn;

		private final BinaryExternalMerger merger;

		private MergingThread(
				ExceptionHandler<IOException> exceptionHandler,
				CircularQueues queues,
				int maxNumFileHandles,
				BinaryExternalMerger merger) {
			super(exceptionHandler, "SortMerger merging thread", queues);
			this.maxFanIn = maxNumFileHandles;
			this.merger = merger;
		}

		@Override
		public void go() throws IOException {

			final List<ChannelWithMeta> spillChannelIDs = new ArrayList<>();
			List<ChannelWithMeta> finalMergeChannelIDs = new ArrayList<>();
			ChannelWithMeta channelID;

			while (isRunning()) {
				try {
					channelID = this.queues.merge.take();
				} catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Merging thread was interrupted (without being shut down) " +
							"while grabbing a channel with meta. Retrying...");
						continue;
					} else {
						return;
					}
				}

				if (!isRunning()) {
					return;
				}
				if (channelID == FINAL_MERGE_MARKER) {
					finalMergeChannelIDs.addAll(spillChannelIDs);
					spillChannelIDs.clear();
					// sort file channels by block numbers, to ensure a better merging performance
					finalMergeChannelIDs.sort(Comparator.comparingInt(ChannelWithMeta::getBlockCount));
					break;
				}

				spillChannelIDs.add(channelID);
				// if async merge is disabled, we will only do the final merge
				// otherwise we wait for `maxFanIn` number of channels to begin a merge
				if (!asyncMergeEnable || spillChannelIDs.size() < maxFanIn) {
					continue;
				}

				// perform a intermediate merge
				finalMergeChannelIDs.addAll(merger.mergeChannelList(spillChannelIDs));
				spillChannelIDs.clear();
			}

			// check if we have spilled some data at all
			if (finalMergeChannelIDs.isEmpty()) {
				if (iterator == null) {
					// only set the iterator if it's not set
					// by the in memory merge stage of spilling thread.
					setResultIterator(EmptyMutableObjectIterator.get());
				}
			} else {
				// merge channels until sufficient file handles are available
				while (isRunning() && finalMergeChannelIDs.size() > this.maxFanIn) {
					finalMergeChannelIDs = merger.mergeChannelList(finalMergeChannelIDs);
				}

				// Beginning final merge.

				// no need to call `getReadMemoryFromHeap` again,
				// because `finalMergeChannelIDs` must become smaller

				List<FileIOChannel> openChannels = new ArrayList<>();
				BinaryMergeIterator<BinaryRowData> iterator = merger.getMergingIterator(
					finalMergeChannelIDs, openChannels);
				channelManager.addOpenChannels(openChannels);

				setResultIterator(iterator);
			}

			// Merging thread done.
		}
	}

	public long getUsedMemoryInBytes() {
		long usedSizeInBytes = 0;
		for (BinaryInMemorySortBuffer sortBuffer : sortBuffers) {
			usedSizeInBytes += sortBuffer.getOccupancy();
		}
		return usedSizeInBytes;
	}

	public long getNumSpillFiles() {
		return numSpillFiles;
	}

	public long getSpillInBytes() {
		return spillInBytes;
	}
}
