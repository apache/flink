/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Tests for {@link KeyGroupPartitionedSnapshotImpl}.
 */
public class AbstractKeyGroupPartitionedSnapshotTest {

	@Test
	public void testWriteMappingsInKeyGroup() throws IOException {
		doTestWriteMappingsInKeyGroup(KeyGroupRange.of(0, 0));
		doTestWriteMappingsInKeyGroup(KeyGroupRange.of(0, 3));
		doTestWriteMappingsInKeyGroup(KeyGroupRange.of(1, 2));
		doTestWriteMappingsInKeyGroup(KeyGroupRange.of(3, 8));
	}

	private void doTestWriteMappingsInKeyGroup(KeyGroupRange testRange) throws IOException {
		int[] offsets = new int[testRange.getNumberOfKeyGroups()];
		Integer[] data = new Integer[gaussSumZeroToX(offsets.length)];

		int pos = 0;
		for (int i = 0; i < offsets.length; ++i) {

			int endIdx = gaussSumZeroToX(i);
			offsets[i] = endIdx;

			while (pos < endIdx) {
				data[pos] = i;
				++pos;
			}
		}

		AbstractKeyGroupPartitioner.PartitioningResult<Integer> result =
			new AbstractKeyGroupPartitioner.PartitioningResult<>(testRange.getStartKeyGroup(), offsets, data);

		final TestElementWriter testElementWriter = new TestElementWriter(result.getNumberOfKeyGroups());
		StateSnapshot.KeyGroupPartitionedSnapshot testInstance =
			new KeyGroupPartitionedSnapshotImpl<>(result, testElementWriter);
		DataOutputView dummyOut = new DataOutputViewStreamWrapper(new ByteArrayOutputStreamWithPos());

		for (Integer keyGroup : testRange) {
			testInstance.writeMappingsInKeyGroup(dummyOut, keyGroup);
		}

		testElementWriter.validateCounts();
	}

	/**
	 * Simple test implementation with validation of {@link KeyGroupPartitionedSnapshotImpl.ElementWriterFunction}.
	 */
	static final class TestElementWriter implements KeyGroupPartitionedSnapshotImpl.ElementWriterFunction<Integer> {

		@Nonnull
		private final int[] countCheck;

		TestElementWriter(@Nonnegative int numberOfKeyGroups) {
			this.countCheck = new int[numberOfKeyGroups];
		}

		@Override
		public void writeElement(@Nonnull Integer element, @Nonnull DataOutputView dov) {
			++countCheck[element];
		}

		void validateCounts() {
			for (int i = 0; i < countCheck.length; ++i) {
				Assert.assertEquals("Key-group count does not match expectation.", i, countCheck[i]);
			}
		}
	}

	private static int gaussSumZeroToX(int x) {
		return (x * (x + 1)) / 2;
	}
}
