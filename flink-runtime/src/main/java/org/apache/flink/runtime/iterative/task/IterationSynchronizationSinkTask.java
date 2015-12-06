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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.types.IntValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.iterative.event.AllWorkersDoneEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.iterative.event.WorkerDoneEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Value;

import com.google.common.base.Preconditions;

/**
 * The task responsible for synchronizing all iteration heads, implemented as an output task. This task
 * will never see any data.
 * In each superstep, it simply waits until it has received a {@link WorkerDoneEvent} from each head and will send back
 * an {@link AllWorkersDoneEvent} to signal that the next superstep can begin.
 */
public class IterationSynchronizationSinkTask extends AbstractInvokable implements Terminable {

	private static final Logger log = LoggerFactory.getLogger(IterationSynchronizationSinkTask.class);

	private MutableRecordReader<IntValue> headEventReader;
	
	private SyncEventHandler eventHandler;

	private ConvergenceCriterion<Value> convergenceCriterion;
	
	private Map<String, Aggregator<?>> aggregators;

	private String convergenceAggregatorName;

	private int currentIteration = 1;
	
	private int maxNumberOfIterations;

	private final AtomicBoolean terminated = new AtomicBoolean(false);


	// --------------------------------------------------------------------------------------------
	
	@Override
	public void registerInputOutput() {
		this.headEventReader = new MutableRecordReader<IntValue>(getEnvironment().getInputGate(0));
	}

	@Override
	public void invoke() throws Exception {
		TaskConfig taskConfig = new TaskConfig(getTaskConfiguration());
		
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
		eventHandler = new SyncEventHandler(numEventsTillEndOfSuperstep, aggregators,
				getEnvironment().getUserClassLoader());
		headEventReader.registerTaskEventListener(eventHandler, WorkerDoneEvent.class);

		IntValue dummy = new IntValue();
		
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

			if (checkForConvergence()) {
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

				AllWorkersDoneEvent allWorkersDoneEvent = new AllWorkersDoneEvent(aggregators);
				sendToAllWorkers(allWorkersDoneEvent);
				
				// reset all aggregators
				for (Aggregator<?> agg : aggregators.values()) {
					agg.reset();
				}
				
//				notifyMonitor(IterationMonitoring.Event.SYNC_FINISHED, currentIteration);
				currentIteration++;
			}
		}
	}

//	protected void notifyMonitor(IterationMonitoring.Event event, int currentIteration) {
//		if (log.isInfoEnabled()) {
//			log.info(IterationMonitoring.logLine(getEnvironment().getJobID(), event, currentIteration, 1));
//		}
//	}

	private boolean checkForConvergence() {
		if (maxNumberOfIterations == currentIteration) {
			if (log.isInfoEnabled()) {
				log.info(formatLogString("maximum number of iterations [" + currentIteration
					+ "] reached, terminating..."));
			}
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
				if (log.isInfoEnabled()) {
					log.info(formatLogString("convergence reached after [" + currentIteration
						+ "] iterations, terminating..."));
				}
				return true;
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
		return BatchTask.constructLogString(message, getEnvironment().getTaskInfo().getTaskName(), this);
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
