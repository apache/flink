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
import org.apache.flink.streaming.runtime.io.StreamTwoInputSelectableProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

/**
 * A {@link StreamTask} for executing the {@link TwoInputStreamOperator} which can select the input to
 * get the next {@link StreamRecord}.
 */
@Internal
public class TwoInputSelectableStreamTask<IN1, IN2, OUT> extends AbstractTwoInputStreamTask<IN1, IN2, OUT> {

	private volatile StreamTwoInputSelectableProcessor<IN1, IN2> inputProcessor;

	public TwoInputSelectableStreamTask(Environment env) {
		super(env);
	}

	@Override
	protected void init(
		Collection<InputGate> inputGates1,
		Collection<InputGate> inputGates2,
		TypeSerializer<IN1> inputDeserializer1,
		TypeSerializer<IN2> inputDeserializer2) {

		this.inputProcessor = new StreamTwoInputSelectableProcessor<>(
			inputGates1, inputGates2,
			inputDeserializer1, inputDeserializer2,
			this,
			getEnvironment().getIOManager(),
			getStreamStatusMaintainer(),
			this.headOperator,
			input1WatermarkGauge,
			input2WatermarkGauge);
	}

	@Override
	protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final StreamTwoInputSelectableProcessor<IN1, IN2> inputProcessor = this.inputProcessor;

		inputProcessor.init();
		while (running && inputProcessor.processInput()) {
			// all the work happens in the "processInput" method
		}
	}

	@Override
	protected void cleanup() {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
		if (inputProcessor != null) {
			inputProcessor.stop();
		}
	}
}
