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


package org.apache.flink.runtime.io.disk;

import java.io.EOFException;
import java.io.IOException;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memorymanager.AbstractMemorySegmentOutputView;
import org.apache.flink.runtime.util.MathUtils;


/**
 *
 *
 */
public class RandomAccessOutputView extends AbstractMemorySegmentOutputView {
	private final MemorySegment[] segments;
	
	private int currentSegmentIndex;
	
	private final int segmentSizeBits;
	
	private final int segmentSizeMask;
	

	public RandomAccessOutputView(MemorySegment[] segments, int segmentSize) {
		this(segments, segmentSize, MathUtils.log2strict(segmentSize));
	}
	
	public RandomAccessOutputView(MemorySegment[] segments, int segmentSize, int segmentSizeBits) {
		super(segments[0], segmentSize, 0);
		
		if ((segmentSize & (segmentSize - 1)) != 0) {
			throw new IllegalArgumentException("Segment size must be a power of 2!");
		}
		
		this.segments = segments;
		this.segmentSizeBits = segmentSizeBits;
		this.segmentSizeMask = segmentSize - 1;
	}

	@Override
	protected void advance() throws IOException {
		if (++this.currentSegmentIndex < this.segments.length) {
			currentSegment = segments[currentSegmentIndex];
			positionInSegment = headerLength;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public long tell() {
		return currentSegmentIndex * segmentSize + positionInSegment;
	}

	@Override
	public void seek(long position) {
		final int bufferNum = (int) (position >>> this.segmentSizeBits);
		final int offset = (int) (position & this.segmentSizeMask);

		this.currentSegmentIndex = bufferNum;
		currentSegment = segments[currentSegmentIndex];
		positionInSegment = offset;
	}
}
