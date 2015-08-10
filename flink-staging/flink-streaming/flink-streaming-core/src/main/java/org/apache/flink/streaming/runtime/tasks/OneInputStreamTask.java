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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(OneInputStreamTask.class);

	private StreamInputProcessor<IN> inputProcessor;

	@Override
	public void registerInputOutput() {
		try {
			super.registerInputOutput();
			
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
		catch (Exception e) {
			throw new RuntimeException("Failed to initialize stream operator: " + e.getMessage(), e);
		}
	}

	@Override
	public void invoke() throws Exception {
		boolean operatorOpen = false;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Task {} invoked", getName());
		}

		try {
			openOperator();
			operatorOpen = true;

			while (inputProcessor.processInput(streamOperator)) {
				// nothing to do, just keep processing
			}

			closeOperator();
			operatorOpen = false;

			if (LOG.isDebugEnabled()) {
				LOG.debug("Task {} invocation finished", getName());
			}

		}
		catch (Exception e) {
			LOG.error(getEnvironment().getTaskNameWithSubtasks() + " failed", e);
			
			if (operatorOpen) {
				try {
					closeOperator();
				} catch (Throwable t) {
					LOG.warn("Exception while closing operator.", t);
				}
			}
			
			throw e;
		}
		finally {
			this.isRunning = false;
			// Cleanup
			inputProcessor.clearBuffers();
			inputProcessor.cleanup();
			outputHandler.flushOutputs();
			clearBuffers();
		}

	}
}
