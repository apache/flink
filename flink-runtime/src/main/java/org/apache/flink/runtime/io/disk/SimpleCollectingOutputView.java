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
import java.io.IOException;
import java.util.List;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MathUtils;

/**
 * The list with the full segments contains at any point all completely full segments, plus the segment that is
 * currently filled.
 */
public class SimpleCollectingOutputView extends AbstractPagedOutputView {
	
	private final List<MemorySegment> fullSegments;
	
	private final MemorySegmentSource memorySource;
	
	private final int segmentSizeBits;
	
	private int segmentNum;


	public SimpleCollectingOutputView(List<MemorySegment> fullSegmentTarget, 
									MemorySegmentSource memSource, int segmentSize)
	{
		super(memSource.nextSegment(), segmentSize, 0);
		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.fullSegments = fullSegmentTarget;
		this.memorySource = memSource;
		this.fullSegments.add(getCurrentSegment());
	}
	
	
	public void reset() {
		if (this.fullSegments.size() != 0) {
			throw new IllegalStateException("The target list still contains memory segments.");
		}
	
		clear();
		try {
			advance();
		} catch (IOException ioex) {
			throw new RuntimeException("Error getting first segment for record collector.", ioex);
		}
		this.segmentNum = 0;
	}
	
	@Override
	protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
		final MemorySegment next = this.memorySource.nextSegment();
		if (next != null) {
			this.fullSegments.add(next);
			this.segmentNum++;
			return next;
		} else {
			throw new EOFException("Can't collect further: memorySource depleted");
		}
	}
	
	public long getCurrentOffset() {
		return (((long) this.segmentNum) << this.segmentSizeBits) + getCurrentPositionInSegment();
	}
}
