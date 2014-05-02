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

import java.util.Map;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;
import eu.stratosphere.types.Value;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

/**
* a delegating {@link MutableObjectIterator} that interrupts the current thread when a given number of events occured.
* This is necessary to repetitively read channels when executing iterative data flows. The wrapped iterator must return
* false
* on interruption, see {@link eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors}
*/
public class SyncEventHandler implements EventListener {
	
//	private static final Log log = LogFactory.getLog(SyncEventHandler.class);
	
	private final ClassLoader userCodeClassLoader;
	
	private final Map<String, Aggregator<?>> aggregators;

	private final int numberOfEventsUntilEndOfSuperstep;

	private int workerDoneEventCounter;
	
	private boolean endOfSuperstep;


	public SyncEventHandler(int numberOfEventsUntilEndOfSuperstep, Map<String, Aggregator<?>> aggregators, ClassLoader userCodeClassLoader) {
		Preconditions.checkArgument(numberOfEventsUntilEndOfSuperstep > 0);
		this.userCodeClassLoader = userCodeClassLoader;
		this.numberOfEventsUntilEndOfSuperstep = numberOfEventsUntilEndOfSuperstep;
		this.aggregators = aggregators;
	}

	@Override
	public void eventOccurred(AbstractTaskEvent event) {
		if (WorkerDoneEvent.class.equals(event.getClass())) {
			onWorkerDoneEvent((WorkerDoneEvent) event);
			return;
		}
		throw new IllegalStateException("Unable to handle event " + event.getClass().getName());
	}

	private void onWorkerDoneEvent(WorkerDoneEvent workerDoneEvent) {
		if (this.endOfSuperstep) {
			throw new RuntimeException("Encountered WorderDoneEvent when still in End-of-Superstep status.");
		}
		
		workerDoneEventCounter++;
		
//		if (log.isInfoEnabled()) {
//			log.info("Sync event handler received WorkerDoneEvent event (" + workerDoneEventCounter + ")");
//		}
		
		String[] aggNames = workerDoneEvent.getAggregatorNames();
		Value[] aggregates = workerDoneEvent.getAggregates(userCodeClassLoader);

		if (aggNames.length != aggregates.length) {
			throw new RuntimeException("Inconsistent WorkerDoneEvent received!");
		}
		
		for (int i = 0; i < aggNames.length; i++) {
			@SuppressWarnings("unchecked")
			Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregators.get(aggNames[i]);
			aggregator.aggregate(aggregates[i]);
		}

		if (workerDoneEventCounter % numberOfEventsUntilEndOfSuperstep == 0) {
			endOfSuperstep = true;
			Thread.currentThread().interrupt();
		}
	}
	
	public boolean isEndOfSuperstep() {
		return this.endOfSuperstep;
	}
	
	public void resetEndOfSuperstep() {
		this.endOfSuperstep = false;
	}
}
