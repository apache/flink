/**
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


package org.apache.flink.runtime.memorymanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.flink.core.memory.MemorySegment;


/**
 * The base class for all output views that are backed by multiple memory pages which are not always kept in memory
 * but which are supposed to be spilled after being written to. The concrete sub classes must
 * implement the methods to collect the current page and provide the next memory page once the boundary is crossed or
 * once the underlying buffers are unlocked by a call to {@link #unlock()}.
 */
public abstract class AbstractPagedOutputView extends AbstractMemorySegmentOutputView {

	// Can we just return the current segment when we get the next one or do we have to keep
	// them because {@link lock} was called. We need a counter because of nested locking.
	protected int lockCount;

	// The segments that are currently locked. When {@link lock} is called we lock the current
	// segment and all the segments that we "advance" to. Only when {@link unlock} is called can we return
	// all of the segments but the most recent one.
	private List<MemorySegment> lockedSegments;

	// Index of current segment in list of locked segments. When we are not locked this is set to 0, because
	// then we always only have one segment.
	private int currentSegmentIndex;

	// --------------------------------------------------------------------------------------------
	//                                    Constructors
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new output view that writes initially to the given initial segment. All segments in the
	 * view have to be of the given {@code segmentSize}. A header of length {@code headerLength} is left
	 * at the beginning of each segment.
	 *
	 * @param initialSegment The segment that the view starts writing to.
	 * @param segmentSize The size of the memory segments.
	 * @param headerLength The number of bytes to skip at the beginning of each segment for the header.
	 */
	protected AbstractPagedOutputView(MemorySegment initialSegment, int segmentSize, int headerLength) {
		super(initialSegment, segmentSize, headerLength);

		this.currentSegment = initialSegment;
		this.positionInSegment = headerLength;

		lockedSegments = new ArrayList<MemorySegment>();

		// at the beginning we only have one segment
		currentSegmentIndex = 0;
		lockCount = 0;
	}

	/**
	 * @param segmentSize The size of the memory segments.
	 * @param headerLength The number of bytes to skip at the beginning of each segment for the header.
	 */
	protected AbstractPagedOutputView(int segmentSize, int headerLength) {
		super(segmentSize, headerLength);

		lockedSegments = new ArrayList<MemorySegment>();
		currentSegmentIndex = 0;
		lockCount = 0;
	}


	// --------------------------------------------------------------------------------------------
	//                                  Page Management
	// --------------------------------------------------------------------------------------------

	/**
	 * This method must return a segment. If no more segments are available, it must throw an
	 * {@link java.io.EOFException}.
	 *
	 * This method and {@link #returnSegment(org.apache.flink.core.memory.MemorySegment, int)} are to be implemented
	 * by child classes to deal with the underlying memory segments.
	 *
	 * @return The next memory segment.
	 *
	 * @throws java.io.IOException
	 */

	protected abstract MemorySegment requestSegment() throws IOException;

	/**
	 * This method returns a filled memory segment to where it came from. Child classes
	 * are responsible for handling the segments.
	 *
	 * @param segment The  memory segment
	 * @param bytesWritten The position in the segment, one after the last valid byte.
	 *
	 * @throws java.io.IOException
	 */
	protected abstract void returnSegment(MemorySegment segment, int bytesWritten) throws IOException;


	/**
	 * Gets the segment that the view currently writes to.
	 *
	 * @return The segment the view currently writes to.
	 */
	public MemorySegment getCurrentSegment() {
		return this.currentSegment;
	}

	/**
	 * Gets the current write position (the position where the next bytes will be written)
	 * in the current memory segment.
	 *
	 * @return The current write offset in the current memory segment.
	 */
	public int getCurrentPositionInSegment() {
		return this.positionInSegment;
	}

	/**
	 * Gets the size of the segments used by this view.
	 *
	 * @return The memory segment size.
	 */
	public int getSegmentSize() {
		return this.segmentSize;
	}

	/**
	 * Moves the output view to the next page. This method invokes internally the
	 * {@link #requestSegment()} and {@link #returnSegment(org.apache.flink.core.memory.MemorySegment, int)} methods
	 * to give the current memory segment to the concrete subclass' implementation and obtain the next segment to
	 * write to. Writing will continue inside the new segment after the header.
	 *
	 * @throws java.io.IOException Thrown, if the current segment could not be processed or a new segment could not
	 *                     be obtained.
	 */
	@Override
	protected void advance() throws IOException {
		if (lockCount > 0) {
			if (currentSegmentIndex < lockedSegments.size() - 1) {
				currentSegmentIndex++;
				currentSegment = lockedSegments.get(currentSegmentIndex);
			} else {
				currentSegmentIndex++;
				currentSegment = requestSegment();
				lockedSegments.add(currentSegment);
			}
		} else {
			returnSegment(currentSegment, positionInSegment);
			currentSegment = requestSegment();
			currentSegmentIndex = 0;
		}
		positionInSegment = headerLength;
	}

	public void lock() {
		lockCount++;
		if (lockCount == 1) {
			// we are the first to lock, so start the "locked segments" list
			Preconditions.checkState(lockedSegments.isEmpty(), "List of locked segments must be empty.");
			Preconditions.checkState(currentSegmentIndex == 0, "Before locking currentSegmentIndex must always be 0.");
			lockedSegments.add(currentSegment);
		}
	}

	// here positions are relative to first locked segment
	public long tell() {
		Preconditions.checkState(lockCount > 0, "Target buffer must be locked.");

		return currentSegmentIndex * segmentSize + positionInSegment;
	}

	public void seek(long position) throws IOException {
		Preconditions.checkState(lockCount > 0, "Target buffer must be locked.");
		Preconditions.checkArgument(position >= 0, "Position must be >= 0 .");

		// Check whether we have to seek into new segments or whether we are simply seeking
		// back into our locked segments
		int positionSegmentIndex = (int) (position / segmentSize);
		int positionInLastSegment = (int) (position % segmentSize);
		if (positionSegmentIndex > currentSegmentIndex) {
			int segmentsToAdvance = positionSegmentIndex - currentSegmentIndex;
			for (int i = 0; i < segmentsToAdvance; i++) {
				// fill all the segments that we seek over (might be random data though)
				advance();
			}
		} else {
			currentSegmentIndex = positionSegmentIndex;
			currentSegment = lockedSegments.get(currentSegmentIndex);
		}
		positionInSegment = positionInLastSegment;
	}

	public void unlock() throws IOException {
		if (lockCount <= 0) {
			throw new IllegalStateException("Unlock call without previous lock.");
		}

		lockCount--;

		if (lockCount == 0) {

			if (currentSegmentIndex != lockedSegments.size() - 1) {
				// Probably a misbehaved serializer that forgot to seek back to the end of the written data
				// after seeking to the beginning to write a header
				throw new IllegalStateException("Current locked segment is not the last locked segment at last unlock.");
			}

			// return every segment but the last one
			int numSegmentsToReturn = lockedSegments.size() - 1;
			for (int i = 0; i < numSegmentsToReturn; i++) {
				returnSegment(lockedSegments.get(i), segmentSize);
			}

			lockedSegments.clear();
			currentSegmentIndex = 0;
		}
	}

	/**
	 * Clears the internal state. Any successive write calls will fail until {@link #advance()}is called.
	 *
	 * @see #advance()
	 */
	protected void clear() {
		this.currentSegment = null;
		this.positionInSegment = this.headerLength;
		currentSegmentIndex = -1;
	}
}
