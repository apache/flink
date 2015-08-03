/*
 * Copyright 2015 EURA NOVA.
 *
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

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.iterative.event.WorkerClockEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.types.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Event handler used by {@link org.apache.flink.runtime.iterative.task.SSPClockSinkTask} to keep track of the individual
 * clocks in the cluster and to broadcast the cluster-wide clock in Stale Synchronous Parallel iterations.
 */
public class SSPClockSyncEventHandler implements EventListener<TaskEvent> {

	private static final Logger log = LoggerFactory.getLogger(SSPClockSyncEventHandler.class);

	private final ClassLoader userCodeClassLoader;

	private final Map<String, Aggregator<?>> aggregators;

	private boolean endOfSuperstep;

	private boolean clockUpdated;

	private boolean aggUpdated;

	private int currentClock = 0;

	private final int numberOfEventsUntilEndOfSuperstep;

	private Map<Integer, Integer> clocks;

	private int lastWorkerIndex = -1;

	public int getLastWorkerIndex() {
		return lastWorkerIndex;
	}


	public SSPClockSyncEventHandler(int numberOfEventsUntilEndOfSuperstep, Map<String, Aggregator<?>> aggregators, ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.aggregators = aggregators;
		this.numberOfEventsUntilEndOfSuperstep = numberOfEventsUntilEndOfSuperstep;
		this.clocks = new HashMap<Integer, Integer>();
	}

	private void workerClock(int workerInt, int clock) {
		this.clocks.put(workerInt, clock);
	}

	public int getCurrentClock() {
		return currentClock;
	}

	@Override
	public void onEvent(TaskEvent event) {

		if (WorkerClockEvent.class.equals(event.getClass())) {
			onWorkerClockEvent((WorkerClockEvent) event);
			return;
		}

		throw new IllegalStateException("Unable to handle event " + event.getClass().getName());
	}

	private void onWorkerClockEvent(WorkerClockEvent workerClockEvent) {
		String[] aggNames = workerClockEvent.getAggregatorNames();
		Value[] aggregates = workerClockEvent.getAggregates(userCodeClassLoader);

		if (aggNames.length != aggregates.length) {
			throw new RuntimeException("Inconsistent WorkerDoneEvent received!");
		}

		int workerIndex = workerClockEvent.getWorkerIndex();
		int workerClock = workerClockEvent.getWorkerClock();

		lastWorkerIndex = workerIndex;

		log.info("Worker " + workerIndex + " is at clock " + workerClock);

		workerClock(workerIndex, workerClock);

		for (int i = 0; i < aggNames.length; i++) {
			@SuppressWarnings("unchecked")
			Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregators.get(aggNames[i]);
			aggregator.aggregate(aggregates[i]);
			this.aggUpdated = true;
		}

		if (clocks.size() % numberOfEventsUntilEndOfSuperstep == 0) { //means all the workers have initialized
			int newClockCandidate = getMinClock();
			if (newClockCandidate > currentClock) {
				currentClock = newClockCandidate;
				this.clockUpdated = true;
			}
		}
		this.endOfSuperstep = true;
		Thread.currentThread().interrupt();
	}

	private int getMinClock() {
		int min = Integer.MAX_VALUE;
		for (Map.Entry<Integer, Integer> e : clocks.entrySet()) {
			int val = e.getValue();
			if (val < min) {
				min = val;
			}
		}
		return min;
	}

	public boolean isEndOfSuperstep() {
		return this.endOfSuperstep;
	}

	public void resetEndOfSuperstep() {
		this.endOfSuperstep = false;
	}

	public boolean isClockUpdated() {
		return this.clockUpdated;
	}

	public void resetClockUpdated() {
		this.clockUpdated = false;
		this.aggUpdated = false;
	}

	public boolean isAggUpdated() {
		return this.aggUpdated;
	}

	public void resetAggUpdated() {
		this.aggUpdated = false;
	}
}
