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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A reducer that aggregates multiple {@link OperatorState}'s into a single {@link Savepoint}.
 */
@Internal
public class MergeOperatorStates implements GroupReduceFunction<OperatorState, Savepoint> {
	private final SavepointMetadata metadata;

	public MergeOperatorStates(SavepointMetadata metadata) {
		Preconditions.checkNotNull(metadata, "Savepoint metadata must not be null");

		this.metadata = metadata;
	}

	@Override
	public void reduce(Iterable<OperatorState> values, Collector<Savepoint> out) {
		Savepoint savepoint =
			new SavepointV2(
				SnapshotUtils.CHECKPOINT_ID,
				StreamSupport.stream(values.spliterator(), false).collect(Collectors.toList()),
				metadata.getMasterStates());

		out.collect(savepoint);
	}
}

