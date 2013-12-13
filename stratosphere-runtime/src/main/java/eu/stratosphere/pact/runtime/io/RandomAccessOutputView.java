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

package eu.stratosphere.pact.runtime.io;

import java.io.EOFException;

import eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView;
import eu.stratosphere.pact.runtime.util.MathUtils;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class RandomAccessOutputView extends AbstractPagedOutputView implements SeekableDataOutputView
{
	private final MemorySegment[] segments;
	
	private int currentSegmentIndex;
	
	private final int segmentSizeBits;
	
	private final int segmentSizeMask;
	
	/**
	 * @param segmentSize
	 * @param headerLength
	 */
	public RandomAccessOutputView(MemorySegment[] segments, int segmentSize)
	{
		this(segments, segmentSize, MathUtils.log2strict(segmentSize));
	}
	
	public RandomAccessOutputView(MemorySegment[] segments, int segmentSize, int segmentSizeBits)
	{
		super(segments[0], segmentSize, 0);
		
		if ((segmentSize & (segmentSize - 1)) != 0)
			throw new IllegalArgumentException("Segment size must be a power of 2!");
		
		this.segments = segments;
		this.segmentSizeBits = segmentSizeBits;
		this.segmentSizeMask = segmentSize - 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment, int)
	 */
	@Override
	protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException
	{
		if (++this.currentSegmentIndex < this.segments.length) {
			return this.segments[this.currentSegmentIndex];
		} else {
			throw new EOFException();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView#setWritePosition(long)
	 */
	@Override
	public void setWritePosition(long position)
	{
		final int bufferNum = (int) (position >>> this.segmentSizeBits);
		final int offset = (int) (position & this.segmentSizeMask);
		
		this.currentSegmentIndex = bufferNum;
		seekOutput(this.segments[bufferNum], offset);
	}
}
