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

import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupPartitionerTestBase;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTable.StateTableEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Random;
import java.util.Set;

/**
 * Test for {@link org.apache.flink.runtime.state.heap.CopyOnWriteStateTableSnapshot.StateTableKeyGroupPartitioner}.
 */
public class StateTableKeyGroupPartitionerTest extends
	KeyGroupPartitionerTestBase<StateTableEntry<Integer, VoidNamespace, Integer>> {

	public StateTableKeyGroupPartitionerTest() {
		super(random -> generateElement(random, null), StateTableEntry::getKey);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected StateTableEntry<Integer, VoidNamespace, Integer>[] generateTestInput(
		Random random,
		int numElementsToGenerate,
		Set<StateTableEntry<Integer, VoidNamespace, Integer>> allElementsIdentitySet) {

		// we let the array size differ a bit from the test size to check this works
		final int arraySize = numElementsToGenerate > 1 ? numElementsToGenerate + 5 : numElementsToGenerate;
		final StateTableEntry<Integer, VoidNamespace, Integer>[] data = new StateTableEntry[arraySize];

		while (numElementsToGenerate > 0) {

			final int generateAsChainCount = Math.min(1 + random.nextInt(3) , numElementsToGenerate);

			StateTableEntry<Integer, VoidNamespace, Integer> element = null;
			for (int i = 0; i < generateAsChainCount; ++i) {
				element = generateElement(random, element);
				allElementsIdentitySet. add(element);
			}

			data[data.length - numElementsToGenerate + random.nextInt(generateAsChainCount)] = element;
			numElementsToGenerate -= generateAsChainCount;
		}

		return data;
	}

	@Override
	protected KeyGroupPartitioner<StateTableEntry<Integer, VoidNamespace, Integer>> createPartitioner(
		StateTableEntry<Integer, VoidNamespace, Integer>[] data,
		int numElements,
		KeyGroupRange keyGroupRange,
		int totalKeyGroups,
		KeyGroupPartitioner.ElementWriterFunction<
			StateTableEntry<Integer, VoidNamespace, Integer>> elementWriterFunction) {

		return new CopyOnWriteStateTableSnapshot.StateTableKeyGroupPartitioner<>(
			data,
			numElements,
			keyGroupRange,
			totalKeyGroups,
			elementWriterFunction);
	}

	private static StateTableEntry<Integer, VoidNamespace, Integer> generateElement(
		@Nonnull Random random,
		@Nullable StateTableEntry<Integer, VoidNamespace, Integer> next) {

		Integer generatedKey =  random.nextInt() & Integer.MAX_VALUE;
		return new StateTableEntry<>(
			generatedKey,
			VoidNamespace.INSTANCE,
			random.nextInt(),
			generatedKey.hashCode(),
			next,
			0,
			0);
	}
}
