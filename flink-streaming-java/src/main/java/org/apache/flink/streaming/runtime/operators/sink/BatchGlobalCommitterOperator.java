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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.operators.util.SinkUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.FunctionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Runtime {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing
 * {@link GlobalCommitter} in the batch execution mode.
 *
 * @param <CommT> The committable type of the {@link GlobalCommitter}
 * @param <GlobalCommT> The committable type of the {@link GlobalCommitter}
 */
final class BatchGlobalCommitterOperator<CommT, GlobalCommT> extends AbstractStreamOperator<GlobalCommT>
		implements OneInputStreamOperator<CommT, GlobalCommT>, BoundedOneInput {

	/** Aggregate committables to global committables and commit the global committables to the external system. */
	private final GlobalCommitter<CommT, GlobalCommT> globalCommitter;

	/** Record all the committables until the end of the input. */
	private final List<CommT> allCommittables;

	/** Re-commit interval at the end of input. */
	private final long retryInterval;

	BatchGlobalCommitterOperator(GlobalCommitter<CommT, GlobalCommT> globalCommitter) {
		this.globalCommitter = checkNotNull(globalCommitter);
		this.allCommittables = new ArrayList<>();
		this.retryInterval = 10_000;
	}

	@Override
	public void processElement(StreamRecord<CommT> element) {
		allCommittables.add(element.getValue());
	}

	@Override
	public void endInput() throws Exception {
		if (!allCommittables.isEmpty()) {
			final GlobalCommT globalCommittable = globalCommitter.combine(allCommittables);

			SinkUtils.commit(
					Collections.singletonList(globalCommittable),
					FunctionUtils.uncheckedFunction(globalCommitter::commit),
					output,
					retryInterval);
		}
		globalCommitter.endOfInput();
	}

	@Override
	public void close() throws Exception {
		super.close();
		globalCommitter.close();
	}
}
