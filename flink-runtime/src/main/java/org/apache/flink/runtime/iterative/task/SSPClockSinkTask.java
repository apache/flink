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

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.iterative.event.AggregatorEvent;
import org.apache.flink.runtime.iterative.event.ClockTaskEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.iterative.event.WorkerClockEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drop-in replacement for {@link org.apache.flink.runtime.iterative.task.IterationSynchronizationSinkTask} for Stale
 * Synchronous Parallel iterations. It keeps track of all the individual clocks in the cluster {@link org.apache.flink.runtime.iterative.event.WorkerClockEvent}
 * and broadcasts the cluster-wide clock values {@link org.apache.flink.runtime.iterative.event.ClockTaskEvent}.
 * A new definition of the convergence criteria is provided and followed in SSP.
 */
public class SSPClockSinkTask extends AbstractInvokable implements Terminable {

	private static final Logger log = LoggerFactory.getLogger(SSPClockSinkTask.class);

	private MutableRecordReader<IntValue> headEventReader;
	
	private SSPClockSyncEventHandler eventHandler;

	private ConvergenceCriterion<Value> convergenceCriterion;
	
	private Map<String, Aggregator<?>> aggregators;

	private String convergenceAggregatorName;

	private int currentIteration = 1;
	
	private int maxNumberOfIterations;

	private final AtomicBoolean terminated = new AtomicBoolean(false);

	private boolean terminate = false;

	private Map<Integer, Boolean> convergenceMap = null;

	private int tasksInParallel = -1;

	// --------------------------------------------------------------------------------------------

	@Override
	public void registerInputOutput() {
		this.headEventReader = new MutableRecordReader<IntValue>(getEnvironment().getInputGate(0));
	}

	@Override
	public void invoke() throws Exception {
		TaskConfig taskConfig = new TaskConfig(getTaskConfiguration());
		//TODO what follows is very ugly. Pass this to the TaskConfiguration instead
		int slack = getExecutionConfig().getSSPSlack()> -1 ? getExecutionConfig().getSSPSlack(): 3;
		tasksInParallel = taskConfig.getNumberOfEventsUntilInterruptInIterativeGate(0);

		// store all aggregators
		this.aggregators = new HashMap<String, Aggregator<?>>();
		for (AggregatorWithName<?> aggWithName : taskConfig.getIterationAggregators(getUserCodeClassLoader())) {
			aggregators.put(aggWithName.getName(), aggWithName.getAggregator());
		}
		
		// store the aggregator convergence criterion
		if (taskConfig.usesConvergenceCriterion()) {
			convergenceCriterion = taskConfig.getConvergenceCriterion(getUserCodeClassLoader());
			convergenceAggregatorName = taskConfig.getConvergenceCriterionAggregatorName();
			Preconditions.checkNotNull(convergenceAggregatorName);
		}
		
		maxNumberOfIterations = taskConfig.getNumberOfIterations();
		
		// set up the event handler
		int numEventsTillEndOfSuperstep = taskConfig.getNumberOfEventsUntilInterruptInIterativeGate(0);
		eventHandler = new SSPClockSyncEventHandler(numEventsTillEndOfSuperstep, aggregators, getEnvironment().getUserClassLoader());
		headEventReader.registerTaskEventListener(eventHandler, WorkerClockEvent.class);

		IntValue dummy = new IntValue();

		convergenceMap = new HashMap<Integer, Boolean>();

		while (!terminationRequested()) {

//			notifyMonitor(IterationMonitoring.Event.SYNC_STARTING, currentIteration);
			if (log.isInfoEnabled()) {
				log.info(formatLogString("starting iteration [" + currentIteration + "]"));
			}

			// this call listens for events until the end-of-superstep is reached
			readHeadEventChannel(dummy);

			if (log.isInfoEnabled()) {
				log.info(formatLogString("finishing iteration [" + currentIteration + "]"));
			}

			if (checkForConvergence(slack)) {
				if (log.isInfoEnabled()) {
					log.info(formatLogString("signaling that all workers are to terminate in iteration ["
						+ currentIteration + "]"));
				}

				requestTermination();
				sendToAllWorkers(new TerminationEvent());
//				notifyMonitor(IterationMonitoring.Event.SYNC_FINISHED, currentIteration);
			} else {
				if (log.isInfoEnabled()) {
					log.info(formatLogString("signaling that all workers are done in iteration [" + currentIteration
						+ "]"));
				}

//				AllWorkersDoneEvent allWorkersDoneEvent = new AllWorkersDoneEvent(aggregators);

				if(eventHandler.isClockUpdated() && !terminate) {
					int currentClock = eventHandler.getCurrentClock();
					ClockTaskEvent clockTaskEvent = new ClockTaskEvent(currentClock, aggregators);
					sendToAllWorkers(clockTaskEvent);
					eventHandler.resetClockUpdated();
					log.info(formatLogString("Clock is now "+ currentClock +" and current iteration is "+ currentIteration));
					currentIteration++;
				}
				else if(eventHandler.isAggUpdated()) {
					AggregatorEvent ae = new AggregatorEvent(aggregators);
					sendToAllWorkers(ae);
					eventHandler.resetAggUpdated();
					log.info(formatLogString("Updating aggregators"));
				}

				// reset all aggregators
				// TODO Warning: in SSP are reset after each worker update
				for (Aggregator<?> agg : aggregators.values()) {
					agg.reset();
				}

//				notifyMonitor(IterationMonitoring.Event.SYNC_FINISHED, currentIteration);
			}
		}
	}

//	protected void notifyMonitor(IterationMonitoring.Event event, int currentIteration) {
//		if (log.isInfoEnabled()) {
//			log.info(IterationMonitoring.logLine(getEnvironment().getJobID(), event, currentIteration, 1));
//		}
//	}

	private boolean checkForConvergence(int slack) {

		if (maxNumberOfIterations == currentIteration) {
			terminate = true;
			if (log.isInfoEnabled()) {
				log.info(formatLogString("maximum number of iterations [" + currentIteration
						+ "] reached, terminating..."));}

			return true;
		}

		if (convergenceAggregatorName != null) {
			@SuppressWarnings("unchecked")
			Aggregator<Value> aggregator = (Aggregator<Value>) aggregators.get(convergenceAggregatorName);
			if (aggregator == null) {
				throw new RuntimeException("Error: Aggregator for convergence criterion was null.");
			}

			Value aggregate = aggregator.getAggregate();

			if (convergenceCriterion.isConverged(currentIteration, aggregate)) {
				convergenceMap.put(eventHandler.getLastWorkerIndex(), true);

				if(convergenceMap.size() == tasksInParallel) {
					for(Map.Entry<Integer, Boolean> e : convergenceMap.entrySet()) {
						if(e.getValue() == false) {
							return false;
						}
					}
					if (log.isInfoEnabled()) {
						log.info(formatLogString("convergence reached after [" + currentIteration
								+ "] iterations, terminating..."));
					}
					return true;
				}
			}
			else {
				convergenceMap.put(eventHandler.getLastWorkerIndex(), false);
			}
		}
		
		return false;
	}

	private void readHeadEventChannel(IntValue rec) throws IOException {
		// reset the handler
		eventHandler.resetEndOfSuperstep();
		
		// read (and thereby process all events in the handler's event handling functions)
		try {
			while (this.headEventReader.next(rec)) {
				throw new RuntimeException("Synchronization task must not see any records!");
			}
		} catch (InterruptedException iex) {
			// sanity check
			if (!(eventHandler.isEndOfSuperstep())) {
				throw new RuntimeException("Event handler interrupted without reaching end-of-superstep.");
			}
		}
	}

	private void sendToAllWorkers(TaskEvent event) throws IOException, InterruptedException {
		headEventReader.sendTaskEvent(event);
	}

	private String formatLogString(String message) {
		return RegularPactTask.constructLogString(message, getEnvironment().getTaskName(), this);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean terminationRequested() {
		return terminated.get();
	}

	@Override
	public void requestTermination() {
		terminated.set(true);
	}

}


