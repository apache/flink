/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.task;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.WorksetUpdateOutputCollector;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.util.Collector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * An intermediate iteration task, which runs a {@link PactDriver} inside.
 * <p/>
 * It will propagate {@link EndOfSuperstepEvent}s and {@link TerminationEvent}s to it's connected tasks. Furthermore
 * intermediate tasks can also update the iteration state, either the workset or the solution set.
 * <p/>
 * If the iteration state is updated, the output of this task will be send back to the {@link IterationHeadPactTask} via
 * a {@link BlockingBackChannel} for the workset -XOR- a {@link MutableHashTable} for the solution set. In this case
 * this task must be scheduled on the same instance as the head.
 */
public class IterationIntermediatePactTask<S extends Function, OT> extends AbstractIterativePactTask<S, OT> {

	private static final Log log = LogFactory.getLog(IterationIntermediatePactTask.class);

	private WorksetUpdateOutputCollector<OT> worksetUpdateOutputCollector;

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

		while (this.running && !terminationRequested()) {

			if (log.isInfoEnabled()) {
				log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
			}

			super.run();

			// check if termination was requested
			checkForTerminationAndResetEndOfSuperstepState();

			if (isWorksetUpdate && isWorksetIteration) {
				long numCollected = worksetUpdateOutputCollector.getElementsCollectedAndReset();
				worksetAggregator.aggregate(numCollected);
			}

			if (log.isInfoEnabled()) {
				log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
			}

			if (!terminationRequested()) {
				if (isWorksetUpdate) {
					// notify iteration head if responsible for workset update
					worksetBackChannel.notifyOfEndOfSuperstep();
				}

				// send the end-of-superstep
				sendEndOfSuperstep();

				incrementIterationCounter();
			}
		}
	}

	private void sendEndOfSuperstep() throws IOException, InterruptedException {
		for (AbstractRecordWriter<?> eventualOutput : eventualOutputs) {
			eventualOutput.sendEndOfSuperstep();
		}
	}

}
