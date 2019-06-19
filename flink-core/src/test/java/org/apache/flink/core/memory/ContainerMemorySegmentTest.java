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

package org.apache.flink.core.memory;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test cases for {@link ContainerMemorySegment}.
 */
@RunWith(Parameterized.class)
public class ContainerMemorySegmentTest extends MemorySegmentTestBase {

	public ContainerMemorySegmentTest(int pageSize) {
		super(pageSize);
	}

	@Override
	MemorySegment createSegment(int size) {
		return createSegment(size, null);
	}

	@Override
	MemorySegment createSegment(int size, Object owner) {
		int subSegmentSize = findLargestPower2Multiple(size);
		int subSegmentCount = size / subSegmentSize;

		MemorySegment[] subSegments = new MemorySegment[subSegmentCount];
		for (int i = 0; i < subSegmentCount; i++) {
			subSegments[i] = new HybridMemorySegment(new byte[subSegmentSize]);
		}

		MemorySegment containerSegment = new ContainerMemorySegment(owner, subSegments);
		return containerSegment;
	}

	private int findLargestPower2Multiple(int size) {
		int segSize = 2;
		while (size % segSize == 0) {
			segSize *= 2;
		}
		segSize /= 2;
		if (segSize == size) {
			segSize /= 2;
		}
		return segSize;
	}

	@Test
	@Ignore
	@Override
	public void testByteBufferWrapping() {
	}

	@Test
	@Ignore
	@Override
	public void testCheckAgainstOverflowUnderflowOnRelease() {
	}

	@Test
	@Ignore
	@Override
	public void testByteBufferOutOfBounds() {
	}
}
