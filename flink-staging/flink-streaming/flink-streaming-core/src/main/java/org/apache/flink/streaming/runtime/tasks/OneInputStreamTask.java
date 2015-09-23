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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;

public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private StreamInputProcessor<IN> inputProcessor;
	
	private volatile boolean running = true;

	@Override
	public void init() throws Exception {
		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());
		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs > 0) {
			InputGate[] inputGates = getEnvironment().getAllInputGates();
			inputProcessor = new StreamInputProcessor<IN>(inputGates, inSerializer,
					getCheckpointBarrierListener(), 
					configuration.getCheckpointMode(),
					getEnvironment().getIOManager(),
					getExecutionConfig().areTimestampsEnabled());

			// make sure that stream tasks report their I/O statistics
			AccumulatorRegistry registry = getEnvironment().getAccumulatorRegistry();
			AccumulatorRegistry.Reporter reporter = registry.getReadWriteReporter();
			inputProcessor.setReporter(reporter);
		}
	}

	@Override
	protected void run() throws Exception {
		while (running && inputProcessor.processInput(streamOperator, lock)) {
			if (timerException != null) {
				throw timerException;
			}
		}
	}

	@Override
	protected void cleanup() throws Exception {
		inputProcessor.cleanup();
	}

	@Override
	protected void cancelTask() {
		running = false;
	}
}
