/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.task;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * A task which participates in an iteration and runs a {@link eu.stratosphere.pact.runtime.task.PactDriver} inside. It
 * will propagate {@link EndOfSuperstepEvent}s and {@link TerminationEvent}s to it's connected tasks.
 */
public class IterationIntermediatePactTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT> {

	private static final Log log = LogFactory.getLog(IterationIntermediatePactTask.class);

	@Override
	public void run() throws Exception {

		while (this.running && !terminationRequested()) {

			notifyMonitor(IterationMonitoring.Event.INTERMEDIATE_STARTING);
			if (log.isInfoEnabled()) {
				log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
			}

			notifyMonitor(IterationMonitoring.Event.INTERMEDIATE_PACT_STARTING);

			super.run();

			notifyMonitor(IterationMonitoring.Event.INTERMEDIATE_PACT_FINISHED);
			if (log.isInfoEnabled()) {
				log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
			}

			if (!terminationRequested()) {
				propagateEvent(EndOfSuperstepEvent.INSTANCE);
				incrementIterationCounter();
			} else {
				propagateEvent(TerminationEvent.INSTANCE);
			}
			notifyMonitor(IterationMonitoring.Event.INTERMEDIATE_FINISHED);
		}
	}

	private void propagateEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
		if (log.isInfoEnabled()) {
			log.info(formatLogString("propagating " + event.getClass().getSimpleName()));
		}
		for (AbstractRecordWriter<?> eventualOutput : eventualOutputs) {
			eventualOutput.publishEvent(event);
		}
	}

}
