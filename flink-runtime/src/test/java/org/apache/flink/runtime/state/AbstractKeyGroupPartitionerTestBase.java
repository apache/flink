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

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Abstract test base for implementations of {@link AbstractKeyedStateBackend}.
 */
public abstract class AbstractKeyGroupPartitionerTestBase<T> extends TestLogger {

	@Test
	public void testPartitionByKeyGroup() {

		final Random random = new Random(0x42);
		testPartitionByKeyGroupForSize(0, random);
		testPartitionByKeyGroupForSize(1, random);
		testPartitionByKeyGroupForSize(2, random);
		testPartitionByKeyGroupForSize(1000, random);
	}

	@SuppressWarnings("unchecked")
	private void testPartitionByKeyGroupForSize(int testSize, Random random) {

		final T[] data = generateTestData(random, testSize);

		// Test with 5 key-groups.
		final KeyGroupRange range = new KeyGroupRange(0, 4);
		final int numberOfKeyGroups = range.getNumberOfKeyGroups();
		final AbstractKeyGroupPartitioner<T> testInstance = createPartitioner(data, testSize, range, numberOfKeyGroups);
		final AbstractKeyGroupPartitioner.PartitioningResult<T> result = testInstance.partitionByKeyGroup();

		final T[] partitionedElements = result.getPartitionedElements();

		Assert.assertEquals(numberOfKeyGroups, result.getNumberOfKeyGroups());
		Assert.assertEquals(range.getStartKeyGroup(), result.getFirstKeyGroup());
		Assert.assertEquals(data.length, partitionedElements.length);
		Assert.assertEquals(0, result.getKeyGroupStartOffsetInclusive(range.getStartKeyGroup()));
		Assert.assertEquals(testSize, result.getKeyGroupEndOffsetExclusive(range.getEndKeyGroup()));

		for (int keyGroup = 0; keyGroup < result.getNumberOfKeyGroups(); ++keyGroup) {
			int start = result.getKeyGroupStartOffsetInclusive(keyGroup);
			int end = result.getKeyGroupEndOffsetExclusive(keyGroup);
			for (int i = start; i < end; ++i) {
				Assert.assertEquals("Mismatch at index " + i,
					keyGroup,
					KeyGroupRangeAssignment.assignToKeyGroup(
						testInstance.extractKeyFromElement(partitionedElements[i]),
						numberOfKeyGroups));
			}
		}
	}

	abstract protected T[] generateTestData(Random random, int numElementsToGenerate);

	abstract protected AbstractKeyGroupPartitioner<T> createPartitioner(
		T[] data,
		int numElements,
		KeyGroupRange keyGroupRange,
		int totalKeyGroups);
}
