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

package org.apache.flink.runtime.iterative.task;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannel;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatch;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatchBroker;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.iterative.io.WorksetUpdateOutputCollector;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An intermediate iteration task, which runs a {@link org.apache.flink.runtime.operators.Driver} inside.
 *
 * <p>It will propagate {@link EndOfSuperstepEvent}s and {@link TerminationEvent}s to its connected tasks. Furthermore
 * intermediate tasks can also update the iteration state, either the workset or the solution set.
 *
 * <p>If the iteration state is updated, the output of this task will be send back to the {@link IterationHeadTask} via
 * a {@link BlockingBackChannel} for the workset -XOR- a HashTable for the solution set. In this case
 * this task must be scheduled on the same instance as the head.
 */
public class IterationIntermediateTask<S extends Function, OT> extends AbstractIterativeTask<S, OT> {

	private static final Logger log = LoggerFactory.getLogger(IterationIntermediateTask.class);

	private WorksetUpdateOutputCollector<OT> worksetUpdateOutputCollector;

	// --------------------------------------------------------------------------------------------

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public IterationIntermediateTask(Environment environment) {
		super(environment);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	protected void initialize() throws Exception {
		super.initialize();

		// set the last output collector of this task to reflect the iteration intermediate state update
		// a) workset update
		// b) solution set update
		// c) none

		Collector<OT> delegate = getLastOutputCollector();
		if (isWorksetUpdate) {
			// sanity check: we should not have a solution set and workset update at the same time
			// in an intermediate task
			if (isSolutionSetUpdate) {
				throw new IllegalStateException("Plan bug: Intermediate task performs workset and solutions set update.");
			}

			Collector<OT> outputCollector = createWorksetUpdateOutputCollector(delegate);

			// we need the WorksetUpdateOutputCollector separately to count the collected elements
			if (isWorksetIteration) {
				worksetUpdateOutputCollector = (WorksetUpdateOutputCollector<OT>) outputCollector;
			}

			setLastOutputCollector(outputCollector);
		} else if (isSolutionSetUpdate) {
			setLastOutputCollector(createSolutionSetUpdateOutputCollector(delegate));
		}
	}

	@Override
	public void run() throws Exception {

		SuperstepKickoffLatch nextSuperstepLatch = SuperstepKickoffLatchBroker.instance().get(brokerKey());

		while (this.running && !terminationRequested()) {

			if (log.isInfoEnabled()) {
				log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
			}

			super.run();

			// check if termination was requested
			verifyEndOfSuperstepState();

			if (isWorksetUpdate && isWorksetIteration) {
				long numCollected = worksetUpdateOutputCollector.getElementsCollectedAndReset();
				worksetAggregator.aggregate(numCollected);
			}

			if (log.isInfoEnabled()) {
				log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
			}

			// let the successors know that the end of this superstep data is reached
			sendEndOfSuperstep();

			if (isWorksetUpdate) {
				// notify iteration head if responsible for workset update
				worksetBackChannel.notifyOfEndOfSuperstep();
			}

			boolean terminated = nextSuperstepLatch.awaitStartOfSuperstepOrTermination(currentIteration() + 1);

			if (terminated) {
				requestTermination();
			}
			else {
				incrementIterationCounter();
			}
		}
	}

	private void sendEndOfSuperstep() throws IOException, InterruptedException {
		for (RecordWriter eventualOutput : this.eventualOutputs) {
			eventualOutput.broadcastEvent(EndOfSuperstepEvent.INSTANCE);
		}
	}

}
