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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Describes inputs and outputs information of a task.
 */
public class TaskInputsOutputsDescriptor {

	private final Map<IntermediateDataSetID, Integer> numbersOfInputGateChannels = new HashMap<>();

	private final Map<IntermediateDataSetID, Integer> numbersOfResultSubpartitions = new HashMap<>();

	private TaskInputsOutputsDescriptor(
			final Map<IntermediateDataSetID, Integer> numbersOfInputGateChannels,
			final Map<IntermediateDataSetID, Integer> numbersOfResultSubpartitions) {

		checkNotNull(numbersOfInputGateChannels);
		checkNotNull(numbersOfResultSubpartitions);

		this.numbersOfInputGateChannels.putAll(numbersOfInputGateChannels);
		this.numbersOfResultSubpartitions.putAll(numbersOfResultSubpartitions);
	}

	public Map<IntermediateDataSetID, Integer> getNumbersOfInputGateChannels() {
		return Collections.unmodifiableMap(numbersOfInputGateChannels);
	}

	public Map<IntermediateDataSetID, Integer> getNumbersOfResultSubpartitions() {
		return Collections.unmodifiableMap(numbersOfResultSubpartitions);
	}

	public static TaskInputsOutputsDescriptor from(
			final Map<IntermediateDataSetID, Integer> numbersOfInputGateChannels,
			final Map<IntermediateDataSetID, Integer> numbersOfResultSubpartitions) {

		return new TaskInputsOutputsDescriptor(numbersOfInputGateChannels, numbersOfResultSubpartitions);
	}
}
