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

package eu.stratosphere.pact.runtime.iterative.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.runtime.iterative.event.NextSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;

/**
 * A resettable one-shot latch.
 */
public class SuperstepBarrier implements EventListener {

	private boolean terminationSignaled = false;
	
	/*
	 * This variable is needed for the head task to have a notion when to reset the latch
	 * after all tasks are informend
	 */
	private AtomicInteger waitCount = new AtomicInteger(0);

	private CountDownLatch latch = new CountDownLatch(1);

	/** setup the barrier, has to be called at the beginning of each superstep */
	public void setup() {
		latch = new CountDownLatch(1);
	}

	/** wait on the barrier */
	public void waitForHead() throws InterruptedException {
		latch.await();
		waitCount.decrementAndGet();
	}
	
	/**
	 * because of race conditions this has to be done before the working tasks
	 * submit there end of superstep states
	 */
	public void registerWaiter() {
		waitCount.incrementAndGet();
	}

	/** barrier will release the waiting thread if an event occurs */
	@Override
	public void eventOccurred(AbstractTaskEvent event) {
		if (event instanceof TerminationEvent) {
			terminationSignaled = true;
		}
		else if (event instanceof NextSuperstepEvent) {
			
		}
		else {
			throw new IllegalArgumentException("Unknown event type.");
		}
		latch.countDown();
	}
	
	public void signalTermination() {
		this.terminationSignaled = true;
	}

	public boolean terminationSignaled() {
		return terminationSignaled;
	}
	
	public int getWaitCount() {
		return this.waitCount.get();
	}
}
