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
 * distributed under the License is distributed on an "AS IS" BASIS
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.iterative.concurrent;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.iterative.event.ClockTaskEvent;
import org.apache.flink.runtime.iterative.event.IterationEventWithAggregators;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.types.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;

/**
 * Drop-in replacement for the SuperStepBarrier held by the {@link org.apache.flink.runtime.iterative.task.IterationHeadPactTask}
 * that enables Stale Synchronous Parallel iterations. This object holds the cluster-wide clock and the current worker clock.
 */
public class SSPClockHolder implements EventListener<TaskEvent> {

	private static final Logger log = LoggerFactory.getLogger(SSPClockHolder.class);

	private final ClassLoader userCodeClassLoader;

	private boolean terminationSignaled = false;

	private boolean hasAggregators;

	private CountDownLatch latch;

	private String[] aggregatorNames;
	private Value[] aggregates;

	private int ownClock = 0;

	private int currentClock = 0;

	private final int slack;

	public boolean isAtSSPLimit() {
		return (ownClock > currentClock + slack);
	}

	public SSPClockHolder(ClassLoader userCodeClassLoader, int slack) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.slack = slack;
	}

	/**
	 * Increase the clock value
	 */
	public void clock() {
		this.ownClock++;

	}

	/**
	 * Get the current worker clock value
	 * @return The current clock value
	 */
	public int getClock() {
		return this.ownClock;
	}


	/**
	 * setup the barrier, has to be called at the beginning of each superstep
	 */
	public void setup() {
		latch = new CountDownLatch(1);
	}

	/**
	 * wait on the barrier
	 */
	public void waitForNextClock() throws InterruptedException {
		if (ownClock > currentClock + slack) {
			log.info("Worker is ahead of slack bound (" + slack + ")  : current clock:" + currentClock + " ownclock: " + ownClock);
			setup();
			latch.await();
		}

		log.info("Worker can continue moving ahead (" + slack + ")  : current clock:" + currentClock + " ownclock: " + ownClock);

	}

	public String[] getAggregatorNames() {
		return aggregatorNames;
	}

	public Value[] getAggregates() {
		return aggregates;
	}

	/**
	 * barrier will release the waiting thread if an event occurs
	 *
	 * @param event
	 */
	@Override
	public void onEvent(TaskEvent event) {
		if (event instanceof TerminationEvent) {
			terminationSignaled = true;
			latch.countDown();
		}

		else if (event instanceof IterationEventWithAggregators) {
			IterationEventWithAggregators iewa = (IterationEventWithAggregators) event;
			aggregatorNames = iewa.getAggregatorNames();
			aggregates = iewa.getAggregates(userCodeClassLoader);
			this.hasAggregators = true;

			if(event instanceof ClockTaskEvent) {
				ClockTaskEvent cte = (ClockTaskEvent) event;
				currentClock = cte.getClock();
				aggregatorNames = cte.getAggregatorNames();
				aggregates = cte.getAggregates(userCodeClassLoader);
				this.hasAggregators = true;
				if (isAtSSPLimit()) {
					return;
				}
				else {
					latch.countDown();
				}
			}
		}
	}

	public boolean terminationSignaled() {
		return terminationSignaled;
	}

	public boolean hasAggregators() {
		return hasAggregators;
	}

	public void resetHasAggregators() {
		this.hasAggregators = false;
	}
}
