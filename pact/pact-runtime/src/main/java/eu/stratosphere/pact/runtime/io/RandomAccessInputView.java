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
import java.util.ArrayList;

import eu.stratosphere.nephele.services.memorymanager.AbstractPagedInputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataInputView;
import eu.stratosphere.pact.runtime.util.MathUtils;


/**
 *
 *
 * @author Stephan Ewen
 */
public class RandomAccessInputView extends AbstractPagedInputView implements SeekableDataInputView
{	
	private final ArrayList<MemorySegment> segments;
	
	private int currentSegmentIndex;
	
	private final int segmentSizeBits;
	
	private final int segmentSizeMask;
	
	private final int segmentSize;
	
	public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize)
	{
		this(segments, segmentSize, MathUtils.log2strict(segmentSize));
	}
	
	public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize, int segmentSizeBits)
	{
		super(segments.get(0), segmentSize, 0);
		this.segments = segments;
		this.currentSegmentIndex = 0;
		this.segmentSize = segmentSize;
		this.segmentSizeBits = segmentSizeBits;
		this.segmentSizeMask = segmentSize - 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.SeekableDataInputView#setReadPosition(long)
	 */
	@Override
	public void setReadPosition(long position)
	{
		final int bufferNum = (int) (position >>> this.segmentSizeBits);
		final int offset = (int) (position & this.segmentSizeMask);
		
		this.currentSegmentIndex = bufferNum;
		seekInput(this.segments.get(bufferNum), offset, this.segmentSize);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.AbstractPagedInputView#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
	 */
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws EOFException
	{
		if (++this.currentSegmentIndex < this.segments.size()) {
			return this.segments.get(this.currentSegmentIndex);
		} else {
			throw new EOFException();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.AbstractPagedInputView#getLimitForSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
	 */
	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return this.segmentSize;
	}
}
