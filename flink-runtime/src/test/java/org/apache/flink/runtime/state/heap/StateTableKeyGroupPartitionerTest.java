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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.AbstractKeyGroupPartitioner;
import org.apache.flink.runtime.state.AbstractKeyGroupPartitionerTestBase;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTable.StateTableEntry;

import java.util.Random;

/**
 * Test for {@link org.apache.flink.runtime.state.heap.CopyOnWriteStateTableSnapshot.StateTableKeyGroupPartitioner}.
 */
public class StateTableKeyGroupPartitionerTest extends
	AbstractKeyGroupPartitionerTestBase<StateTableEntry<Integer, VoidNamespace, Integer>> {

	@Override
	@SuppressWarnings("unchecked")
	protected StateTableEntry<Integer, VoidNamespace, Integer>[] generateTestData(
		Random random,
		int numElementsToGenerate) {

		// we let the array size differ a bit from the test size to check this works
		final int arraySize = numElementsToGenerate > 1 ? numElementsToGenerate + 5 : numElementsToGenerate;
		final StateTableEntry<Integer, VoidNamespace, Integer>[] data = new StateTableEntry[arraySize];
		for (int i = 0; i < numElementsToGenerate; ++i) {
			Integer key = random.nextInt() & Integer.MAX_VALUE;
			data[i] = new CopyOnWriteStateTable.StateTableEntry<>(
				key,
				VoidNamespace.INSTANCE,
				42,
				key.hashCode(),
				null,
				0,
				0);
		}

		return data;
	}

	@Override
	protected AbstractKeyGroupPartitioner<StateTableEntry<Integer, VoidNamespace, Integer>> createPartitioner(
		StateTableEntry<Integer, VoidNamespace, Integer>[] data,
		int numElements,
		KeyGroupRange keyGroupRange,
		int totalKeyGroups) {

		return new CopyOnWriteStateTableSnapshot.StateTableKeyGroupPartitioner<>(
			data,
			numElements,
			keyGroupRange,
			totalKeyGroups);
	}
}
