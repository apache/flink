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

package org.apache.flink.table.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

/**
 * Memory segment container whose memory segment size is a power of 2.
 */
public final class Power2SegmentContainer extends MemorySegmentContainer {

	/**
	 * The mask to determine the offset in the memory segment..
	 */
	protected final int segmentSizeMask;

	/**
	 * The number of bits for the size of the memory segment.
	 * This is used to determine the memory segment index.
	 */
	protected final int segmentSizeBitCount;

	public Power2SegmentContainer(MemorySegment[] segments) {
		super(segments);

		Preconditions.checkArgument((segmentSize & (segmentSize - 1)) == 0,
			"The segment size must be a power of 2.");

		segmentSizeMask = segmentSize - 1;
		segmentSizeBitCount = MathUtils.log2strict(segmentSize);
	}

	protected int getSegmentIndex(int index) {
		return index >>> segmentSizeBitCount;
	}

	protected int getOffsetInSegment(int index) {
		return index & segmentSizeMask;
	}
}
