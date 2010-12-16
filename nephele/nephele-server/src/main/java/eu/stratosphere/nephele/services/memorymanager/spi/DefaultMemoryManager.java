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

package eu.stratosphere.nephele.services.memorymanager.spi;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * Default MemoryManager implementation giving hard memory guarantees. The implementation has the following properties:
 * <ul>
 * <li>arbitrary segment sizes (smaller than 2GB)</li>
 * <li>{@code allocate()} and {@code release()} calls in arbitrary order are supported</li>
 * <li>allocation data is stored as an array list in a dedicated structure</li>
 * <li>first-fit selection strategy</li>
 * <li>automatic reintegration of released segments</li>
 * </ul>
 * This implementation uses internal byte arrays to allocate the required memory and allows allocation sizes greater
 * than 2GB. Due to the fact that the length of a single java byte array is bounded by {@link Integer.MAX_VALUE} (2GB),
 * the manager works 2 dimensional byte array (i.e. with memory chunks). Please be aware that in order to keep the array
 * access methods in the {@link DefaultMemorySegment} fast and simple, the actual allocated memory segments must not
 * exceed 2GB and must be contained in a single memory chunk.
 * 
 * @author Alexander Alexandrov
 */
public class DefaultMemoryManager implements MemoryManager {
	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(DefaultMemoryManager.class);

	private static final long MIN_MEMORY_SIZE = 32 * 1024 * 1024;

	/**
	 * Maximal memory chunk size.
	 */
	private static final int MAX_CHUNK_SIZE = Integer.MAX_VALUE;

	/**
	 * The allocated memory for this memoryManager instance.
	 */
	private byte[][] memory;

	/**
	 * Size of the array chunks (defaults to MAX_CHUNK_SIZE).
	 */
	private long chunkSize;

	/**
	 * A list of free segments ordered by start address.
	 */
	private final LinkedList<FreeSegmentEntry> freeSegments;

	/**
	 * A set of Descriptors for the allocated memory segments.
	 */
	private final Set<MemorySegmentDescriptor> allocatedSegments;

	/**
	 * A boolean flag indicating whether the close() has already been invoked.
	 */
	private boolean isShutDown = false;

	// ------------------------------------------------------------------------
	// Constructors / Destructors
	// ------------------------------------------------------------------------

	/**
	 * Default chunk size constructor.
	 */
	public DefaultMemoryManager(long memorySize) {
		this(memorySize, MAX_CHUNK_SIZE);
	}

	/**
	 * Full options constructor.
	 * 
	 * @param memorySize
	 * @param chunkSize
	 */
	public DefaultMemoryManager(long memorySize, int chunkSize) {
		// initialize the free segments list
		freeSegments = new LinkedList<FreeSegmentEntry>();
		// initialize the allocated segment descriptors set
		allocatedSegments = new HashSet<MemorySegmentDescriptor>();

		// calculate the number of required memory chunks and the length of the last chunk
		int numberOfChunks = (int) (memorySize / chunkSize);
		int lastChunkSize = (int) (memorySize % chunkSize);

		// initialize memory chunks
		this.chunkSize = chunkSize;
		this.memory = new byte[numberOfChunks + 1][];
		for (int i = 0; i <= numberOfChunks; i++) {
			// allocate memory of the specified size
			memory[i] = new byte[(i < numberOfChunks) ? chunkSize : lastChunkSize];
			freeSegments.add(new FreeSegmentEntry(i, i * this.chunkSize, i * this.chunkSize + memory[i].length));
		}
	}

	@Override
	public void finalize() {
		shutdown();
	}

	@Override
	public void shutdown() {
		if (!isShutDown) {
			LOG.debug("Shutting down MemoryManager instance " + toString());

			isShutDown = true;
			memory = null; // release the memory
			chunkSize = 0L;
		}
	}

	// ------------------------------------------------------------------------
	// MemoryManager interface implementation
	// ------------------------------------------------------------------------

	@Override
	synchronized public MemorySegment allocate(int segmentSize) throws MemoryAllocationException {
		if (segmentSize < 1) {
			throw new IllegalArgumentException();
		}

		// traverse the free list to find first fitting segment
		for (int i = 0; i < freeSegments.size(); i++) {
			FreeSegmentEntry entry = freeSegments.get(i);

			if (segmentSize <= entry.end - entry.start) {
				// construct a descriptor for the new segment
				int chunk = (int) (entry.start / chunkSize);
				int start = (int) (entry.start % chunkSize);
				MemorySegmentDescriptor descriptor = new MemorySegmentDescriptor(memory[chunk], chunk, start, start
					+ segmentSize);
				allocatedSegments.add(descriptor);

				// construct the new memory segment
				MemorySegment segment = factory(descriptor);

				// adapt the free segment entry
				entry.start += segmentSize;

				// remove the free segment entry from the list if the remaining
				// size is zero
				if (entry.start == entry.end) {
					freeSegments.remove(i);
				}

				// return the new segment
				return segment;
			}
		}

		throw new MemoryAllocationException();
	}

	@Override
	synchronized public void release(MemorySegment segment) {
		// check if segment is null or has already been freed
		if (segment == null || segment.isFree()) {
			return;
		}

		MemorySegmentDescriptor descriptor = ((DefaultMemorySegment) segment).descriptorReference.get();
		long start = descriptor.chunk * (chunkSize) + descriptor.start;
		long end = descriptor.chunk * (chunkSize) + descriptor.end;

		// adapt the free list:
		// case 1: free list is empty
		if (freeSegments.size() == 0) {
			freeSegments.add(new FreeSegmentEntry(descriptor.chunk, start, end));
		}
		// case 2: segment ends before the first free segment entry
		else if (end <= freeSegments.get(0).start) {
			FreeSegmentEntry firstEntry = freeSegments.get(0);
			if (end == firstEntry.start && descriptor.chunk == firstEntry.chunk) {
				firstEntry.start = start;
			} else {
				freeSegments.addFirst(new FreeSegmentEntry(descriptor.chunk, start, end));
			}
		}
		// case 3: segment starts after the last free segment entry
		else if (start >= freeSegments.get(freeSegments.size() - 1).end) {
			FreeSegmentEntry lastEntry = freeSegments.get(freeSegments.size() - 1);
			if (start == lastEntry.end && descriptor.chunk == lastEntry.chunk) {
				lastEntry.end = end;
			} else {
				freeSegments.addLast(new FreeSegmentEntry(descriptor.chunk, start, end));
			}
		}
		// case 4: segment is between two free segment entries (general case, at
		// least two freeSegments entries is ensured)
		else {
			// search for the pair of free segment entries bounding the segment
			// to be freed
			Iterator<FreeSegmentEntry> it = freeSegments.iterator();
			FreeSegmentEntry left = it.next(), right = it.next();
			int i = 1;
			while (!(start >= left.end && end <= right.start)) {
				left = right;
				right = it.next();
				i++;
			}

			if (start == left.end && end == right.start && left.chunk == right.chunk) {
				// extends left entry up to the end of right entry and remove
				// the right entry
				left.end = right.end;
				freeSegments.remove(i);
			} else if (start == left.end && descriptor.chunk == left.chunk) {
				// extend left entry up to the end of the segment
				left.end = end;
			} else if (end == right.start && descriptor.chunk == right.chunk) {
				// extend right entry up to the start of the segment
				right.start = start;
			} else {
				// add an extra entry between left and right
				freeSegments.add(i, new FreeSegmentEntry(descriptor.chunk, start, end));
			}
		}

		// mark the segment as freed
		segment.free();

		// remove the descriptor from the allocated segments set
		allocatedSegments.remove(descriptor);
	}

	@Override
	public Collection<MemorySegment> allocate(int numberOfSegments, int segmentSize) throws MemoryAllocationException {
		Collection<MemorySegment> segments = new ArrayList<MemorySegment>(numberOfSegments);

		for (int i = 0; i < numberOfSegments; i++) {
			try {
				segments.add(allocate(segmentSize));
			} catch (MemoryAllocationException e) {
				// cleanup initialized segments
				release(segments);

				// re-throw the original cause
				throw e;
			}
		}

		return segments;
	}

	@Override
	public <T extends MemorySegment> void release(Collection<T> segments) {
		if (segments != null) {
			for (MemorySegment segment : segments) {
				release(segment);
			}
		}
	}

	/**
	 * Factory method for MemorySegment implementations.
	 * 
	 * @param descriptor
	 * @return
	 */
	private DefaultMemorySegment factory(MemorySegmentDescriptor descriptor) {
		WeakReference<MemorySegmentDescriptor> descriptorReference = new WeakReference<MemorySegmentDescriptor>(
			descriptor);

		DefaultRandomAccessView randomAccessView = new DefaultRandomAccessView(descriptor);
		DefaultDataInputView inputView = new DefaultDataInputView(descriptor);
		DefaultDataOutputView outputView = new DefaultDataOutputView(descriptor);

		return new DefaultMemorySegment(descriptorReference, randomAccessView, inputView, outputView);
	}

	/**
	 * Entries for the free segments list
	 * 
	 * @author Alexander Alexandrov
	 */
	private static final class FreeSegmentEntry {
		private int chunk;

		private long start;

		private long end;

		public FreeSegmentEntry(int chunk, long start, long end) {
			this.chunk = chunk;
			this.start = start;
			this.end = end;
		}
	}

	/**
	 * MemorySegments wrapping a portion of the backing memory.
	 * 
	 * @author Alexander Alexandrov
	 */
	public static final class MemorySegmentDescriptor {
		protected final byte[] memory;

		protected final int chunk;

		protected final int start;

		protected final int end;

		protected final int size;

		protected MemorySegmentDescriptor(byte[] memory, int chunk, int start, int end) {
			this.memory = memory;
			this.chunk = chunk;
			this.start = start;
			this.end = end;
			this.size = end - start;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Instantiates a memory manager that allocates a given fraction of the total memory.
	 * It will however only allocate so much memory such that at least a given amount of memory remains.
	 * 
	 * @param fraction
	 *        The fraction of the current free space to allocate.
	 * @param minUnreserved
	 *        The minimal amount of memory not allocated by the memory manager.
	 * @return An instance of the MemoryManager with the given amount of memory.
	 */
	public static DefaultMemoryManager getWithHeapFraction(float fraction, long minUnreserved) {
		// first, get the basic memory statistics from the runtime
		// the free memory is the currently free memory (in the current committed block), plus
		// the memory that is currently virtual and uncommitted (max - total)
		Runtime r = Runtime.getRuntime();
		long maximum = r.maxMemory();
		long free = maximum - r.totalMemory() + r.freeMemory();
		long bytes = 0;
		long tenuredFree = -1;

		// in order to prevent allocations of arrays that are too big for the JVM's different memory pools,
		// make sure that the maximum segment size is 70% of the currently free tenure heap
		List<MemoryPoolMXBean> poolBeans = ManagementFactory.getMemoryPoolMXBeans();
		for (MemoryPoolMXBean bean : poolBeans) {
			if (bean.getName().equals("Tenured Gen")) {
				// found the tenured pool
				MemoryUsage usage = bean.getUsage();
				tenuredFree = usage.getMax() - usage.getUsed();
				break;
			}
		}

		long maxSegSize = (tenuredFree == -1) ? Integer.MAX_VALUE : (long) (tenuredFree * 0.8);
		if (maxSegSize > Integer.MAX_VALUE) {
			maxSegSize = Integer.MAX_VALUE;
		}

		if (minUnreserved + MIN_MEMORY_SIZE > free) {
			LOG
				.warn("System has low memory. Allocating the minimal memory manager will leave little memory. Expect performance degradation.");
			bytes = MIN_MEMORY_SIZE;
		} else {
			bytes = (long) (maximum * fraction);

			if (free - bytes < minUnreserved) {
				bytes = free - minUnreserved;
				LOG.warn("Memory manager attempt to allocate " + (long) (maximum * fraction)
					+ " bytes would leave less than " + minUnreserved + " bytes free. Decreasing allocated memory to "
					+ bytes + " bytes.");
			} else if (bytes < MIN_MEMORY_SIZE) {
				bytes = MIN_MEMORY_SIZE;
				LOG.warn("Increasing memory manager space to the minimum of " + (MIN_MEMORY_SIZE / 1024) + "K.");
			}
		}

		LOG.info("Instantiating memory manager with a size of " + bytes + " bytes and a maximal chunk size of "
			+ maxSegSize + " bytes.");

		return new DefaultMemoryManager(bytes, (int) maxSegSize);
	}
}
