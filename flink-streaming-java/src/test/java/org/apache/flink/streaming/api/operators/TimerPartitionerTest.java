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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.AbstractKeyGroupPartitioner;
import org.apache.flink.runtime.state.AbstractKeyGroupPartitionerTestBase;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;

import java.util.Random;

/**
 * Test for {@link org.apache.flink.streaming.api.operators.InternalTimerHeapSnapshot.TimerPartitioner}.
 */
public class TimerPartitionerTest
	extends AbstractKeyGroupPartitionerTestBase<TimerHeapInternalTimer<Integer, VoidNamespace>> {

	@Override
	@SuppressWarnings("unchecked")
	protected TimerHeapInternalTimer<Integer, VoidNamespace>[] generateTestData(
		Random random,
		int numElementsToGenerate) {
		final TimerHeapInternalTimer<Integer, VoidNamespace>[] data = new TimerHeapInternalTimer[numElementsToGenerate];
		for (int i = 0; i < numElementsToGenerate; ++i) {
			Integer key = random.nextInt() & Integer.MAX_VALUE;
			data[i] = new TimerHeapInternalTimer<>(42L, key, VoidNamespace.INSTANCE);
		}
		return data;
	}

	@Override
	protected AbstractKeyGroupPartitioner<TimerHeapInternalTimer<Integer, VoidNamespace>> createPartitioner(
		TimerHeapInternalTimer<Integer, VoidNamespace>[] data,
		int numElements,
		KeyGroupRange keyGroupRange,
		int totalKeyGroups) {
		return new InternalTimerHeapSnapshot.TimerPartitioner<>(data, keyGroupRange, totalKeyGroups);
	}
}
