/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.channelStateNonRescalingRepartitioner;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.empty;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.junit.Assert.fail;

/**
 * Simple test for
 * {@link org.apache.flink.runtime.checkpoint.StateAssignmentOperation#channelStateNonRescalingRepartitioner channelStateNonRescalingRepartitioner}.
 * It checks whether partitioner fails or not using property-based approach.
 */
@RunWith(Parameterized.class)
public class ChannelStateNoRescalingPartitionerTest {

	@Parameters(name = "oldParallelism: {0}, newParallelism: {1}, offsetSize: {2}")
	public static Collection<Object[]> parameters() {
		List<Object[]> params = new ArrayList<>();
		int[] parLevels = {1, 2};
		int[] offsetSizes = {0, 1, 2};
		final List<Function<OperatorSubtaskState, ?>> extractors = Arrays.asList(
			OperatorSubtaskState::getInputChannelState,
			OperatorSubtaskState::getResultSubpartitionState
		);
		// generate all possible combinations of the above parameters
		for (int oldParallelism : parLevels) {
			for (int newParallelism : parLevels) {
				for (int offsetSize : offsetSizes) {
					for (Function<OperatorSubtaskState, ?> stateExtractor : extractors) {
						params.add(new Object[]{oldParallelism, newParallelism, offsetSize, stateExtractor});
					}
				}
			}
		}
		return params;
	}

	private static final OperatorID OPERATOR_ID = new OperatorID();

	private final int oldParallelism;
	private final int newParallelism;
	private final int offsetsSize;
	private final Function<OperatorSubtaskState, ? extends StateObjectCollection<?>> extractState;

	public ChannelStateNoRescalingPartitionerTest(int oldParallelism, int newParallelism, int offsetsSize, Function<OperatorSubtaskState, ? extends StateObjectCollection<?>> extractState) {
		this.oldParallelism = oldParallelism;
		this.newParallelism = newParallelism;
		this.offsetsSize = offsetsSize;
		this.extractState = extractState;
	}

	@Test
	public <T extends AbstractChannelStateHandle<?>> void testNoRescaling() {
		OperatorState state = new OperatorState(OPERATOR_ID, oldParallelism, oldParallelism);
		state.putState(0, new OperatorSubtaskState(
			empty(),
			empty(),
			empty(),
			empty(),
			singleton(new InputChannelStateHandle(new InputChannelInfo(0, 0), new EmptyStreamStateHandle(), getOffset())),
			singleton(new ResultSubpartitionStateHandle(new ResultSubpartitionInfo(0, 0), new EmptyStreamStateHandle(), getOffset()))));
		try {
			// noinspection unchecked
			StateAssignmentOperation.reDistributePartitionableStates(
				singletonList(state),
				newParallelism,
				singletonList(OperatorIDPair.generatedIDOnly(OPERATOR_ID)),
				(Function<OperatorSubtaskState, StateObjectCollection<T>>) this.extractState,
				channelStateNonRescalingRepartitioner("test"));
		} catch (IllegalArgumentException e) {
			if (!shouldFail()) {
				throw e;
			} else {
				return;
			}
		}
		if (shouldFail()) {
			fail("expected to fail for: oldParallelism=" + oldParallelism + ", newParallelism=" + newParallelism + ", offsetsSize=" + offsetsSize + ", extractState=" + extractState);
		}
	}

	private boolean shouldFail() {
		return oldParallelism != newParallelism && offsetsSize > 0;
	}

	private List<Long> getOffset() {
		List<Long> offsets = new ArrayList<>(offsetsSize);
		for (int i = 0; i < offsetsSize; i++) {
			offsets.add(0L);
		}
		return offsets;
	}
}
