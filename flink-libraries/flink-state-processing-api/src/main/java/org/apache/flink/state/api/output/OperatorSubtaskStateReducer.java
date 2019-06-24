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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A reducer that aggregates all {@link OperatorSubtaskState}'s for a particular operator into a
 * single {@link OperatorState}.
 */
@Internal
public class OperatorSubtaskStateReducer
	extends RichGroupReduceFunction<TaggedOperatorSubtaskState, OperatorState> {

	private final String uid;

	private final int maxParallelism;

	public OperatorSubtaskStateReducer(String uid, int maxParallelism) {
		this.uid = uid;
		this.maxParallelism = maxParallelism;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

	@Override
	public void reduce(Iterable<TaggedOperatorSubtaskState> values, Collector<OperatorState> out) {
		List<TaggedOperatorSubtaskState> subtasks = StreamSupport
			.stream(values.spliterator(), false)
			.collect(Collectors.toList());

		OperatorState operatorState = new OperatorState(OperatorIDGenerator.fromUid(uid), subtasks.size(), maxParallelism);

		for (TaggedOperatorSubtaskState value : subtasks) {
			operatorState.putState(value.index, value.state);
		}

		out.collect(operatorState);
	}
}
