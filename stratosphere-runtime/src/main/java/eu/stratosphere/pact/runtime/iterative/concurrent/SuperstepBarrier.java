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

package eu.stratosphere.pact.runtime.iterative.concurrent;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.types.Value;

import java.util.concurrent.CountDownLatch;

/**
 * A resettable one-shot latch.
 */
public class SuperstepBarrier implements EventListener {
	
	private final ClassLoader userCodeClassLoader;

	private boolean terminationSignaled = false;

	private CountDownLatch latch;

	private String[] aggregatorNames;
	private Value[] aggregates;
	
	
	public SuperstepBarrier(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
	}
	

	/** setup the barrier, has to be called at the beginning of each superstep */
	public void setup() {
		latch = new CountDownLatch(1);
	}

	/** wait on the barrier */
	public void waitForOtherWorkers() throws InterruptedException {
		latch.await();
	}

	public String[] getAggregatorNames() {
		return aggregatorNames;
	}
	
	public Value[] getAggregates() {
		return aggregates;
	}

	/** barrier will release the waiting thread if an event occurs */
	@Override
	public void eventOccurred(AbstractTaskEvent event) {
		if (event instanceof TerminationEvent) {
			terminationSignaled = true;
		}
		else if (event instanceof AllWorkersDoneEvent) {
			AllWorkersDoneEvent wde = (AllWorkersDoneEvent) event;
			aggregatorNames = wde.getAggregatorNames();
			aggregates = wde.getAggregates(userCodeClassLoader);
		}
		else {
			throw new IllegalArgumentException("Unknown event type.");
		}

		latch.countDown();
	}

	public boolean terminationSignaled() {
		return terminationSignaled;
	}
}
