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

package org.apache.flink.state.api.input.splits;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.KeyedStateHandle;

import java.util.Collections;
import java.util.List;

/**
 * An input split representing a key-group range from a savepoint.
 */
@Internal
public final class KeyGroupRangeInputSplit implements InputSplit {

	private static final long serialVersionUID = -3715297712294815706L;

	private final List<KeyedStateHandle> managedKeyedState;

	private final List<KeyedStateHandle> rawKeyedState;

	private final int numKeyGroups;

	private final int split;

	public KeyGroupRangeInputSplit(
		List<KeyedStateHandle> managedKeyedState,
		List<KeyedStateHandle> rawKeyedState,
		int numKeyGroups,
		int split) {

		this.managedKeyedState = managedKeyedState;
		this.rawKeyedState = rawKeyedState;

		this.numKeyGroups = numKeyGroups;
		this.split = split;
	}

	@Override
	public int getSplitNumber() {
		return split;
	}

	public PrioritizedOperatorSubtaskState getPrioritizedOperatorSubtaskState() {
		return new PrioritizedOperatorSubtaskState.Builder(
			new OperatorSubtaskState(
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				new StateObjectCollection<>(managedKeyedState),
				new StateObjectCollection<>(rawKeyedState)
			),
			Collections.emptyList()
		).build();
	}

	public int getNumKeyGroups() {
		return numKeyGroups;
	}
}
