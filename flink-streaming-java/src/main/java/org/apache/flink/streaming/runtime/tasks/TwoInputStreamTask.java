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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;

import java.util.Collection;

/**
 * A {@link StreamTask} that executes a {@link TwoInputStreamOperator} but does not support
 * the {@link TwoInputStreamOperator} to select input for reading.
 */
@Internal
public class TwoInputStreamTask<IN1, IN2, OUT> extends AbstractTwoInputStreamTask<IN1, IN2, OUT> {

	private StreamTwoInputProcessor<IN1, IN2> inputProcessor;

	public TwoInputStreamTask(Environment env) {
		super(env);
	}

	@Override
	protected void createInputProcessor(
		Collection<InputGate> inputGates1,
		Collection<InputGate> inputGates2,
		TypeSerializer<IN1> inputDeserializer1,
		TypeSerializer<IN2> inputDeserializer2) throws Exception {

		this.inputProcessor = new StreamTwoInputProcessor<>(
			inputGates1, inputGates2,
			inputDeserializer1, inputDeserializer2,
			this,
			getConfiguration().getCheckpointMode(),
			getCheckpointLock(),
			getEnvironment().getIOManager(),
			getEnvironment().getTaskManagerInfo().getConfiguration(),
			getStreamStatusMaintainer(),
			this.headOperator,
			getEnvironment().getMetricGroup().getIOMetricGroup(),
			input1WatermarkGauge,
			input2WatermarkGauge,
			getTaskNameWithSubtaskAndId(),
			operatorChain);
	}

	@Override
	protected void performDefaultAction(ActionContext context) throws Exception {
		if (!inputProcessor.processInput()) {
			context.allActionsCompleted();
		}
	}

	@Override
	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {

	}
}
