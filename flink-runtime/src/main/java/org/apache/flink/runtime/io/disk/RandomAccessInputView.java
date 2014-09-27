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


package org.apache.flink.runtime.io.disk;

import java.io.EOFException;
import java.util.ArrayList;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.memorymanager.AbstractPagedInputView;
import org.apache.flink.runtime.util.MathUtils;


/**
 *
 *
 */
public class RandomAccessInputView extends AbstractPagedInputView implements SeekableDataInputView
{	
	private final ArrayList<MemorySegment> segments;
	
	private int currentSegmentIndex;
	
	private final int segmentSizeBits;
	
	private final int segmentSizeMask;
	
	private final int segmentSize;
	
	private final int limitInLastSegment;
	
	public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize)
	{
		this(segments, segmentSize, segmentSize);
	}
	
	public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize, int limitInLastSegment)
	{
		super(segments.get(0), segments.size() > 1 ? segmentSize : limitInLastSegment, 0);
		this.segments = segments;
		this.currentSegmentIndex = 0;
		this.segmentSize = segmentSize;
		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.segmentSizeMask = segmentSize - 1;
		this.limitInLastSegment = limitInLastSegment;
	}


	@Override
	public void setReadPosition(long position)
	{
		final int bufferNum = (int) (position >>> this.segmentSizeBits);
		final int offset = (int) (position & this.segmentSizeMask);
		
		this.currentSegmentIndex = bufferNum;
		seekInput(this.segments.get(bufferNum), offset, bufferNum < this.segments.size() - 1 ? this.segmentSize : this.limitInLastSegment);
	}


	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws EOFException
	{
		if (++this.currentSegmentIndex < this.segments.size()) {
			return this.segments.get(this.currentSegmentIndex);
		} else {
			throw new EOFException();
		}
	}


	@Override
	protected int getLimitForSegment(MemorySegment segment)
	{
		return this.currentSegmentIndex == this.segments.size() - 1 ? this.limitInLastSegment : this.segmentSize;
	}
}
