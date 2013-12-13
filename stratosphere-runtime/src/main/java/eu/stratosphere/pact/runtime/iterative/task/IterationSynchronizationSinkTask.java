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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.pact.common.stubs.aggregators.Aggregator;
import eu.stratosphere.pact.common.stubs.aggregators.AggregatorWithName;
import eu.stratosphere.pact.common.stubs.aggregators.ConvergenceCriterion;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;
//import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * The task responsible for synchronizing all iteration heads, implemented as an {@link AbstractOutputTask}. This task
 * will never see any data.
 * In each superstep, it simply waits until it has receiced a {@link WorkerDoneEvent} from each head and will send back
 * an {@link AllWorkersDoneEvent} to signal that the next superstep can begin.
 */
public class IterationSynchronizationSinkTask extends AbstractOutputTask implements Terminable {

	private static final Log log = LogFactory.getLog(IterationSynchronizationSinkTask.class);

	private MutableRecordReader<IntegerRecord> headEventReader;
	
	private ClassLoader userCodeClassLoader;
	
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
		this.headEventReader = new MutableRecordReader<IntegerRecord>(this);
	}

	@Override
	public void invoke() throws Exception {
		userCodeClassLoader = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
		TaskConfig taskConfig = new TaskConfig(getTaskConfiguration());
		
		// instantiate all aggregators
		this.aggregators = new HashMap<String, Aggregator<?>>();
		for (AggregatorWithName<?> aggWithName : taskConfig.getIterationAggregators()) {
			Aggregator<?> agg = InstantiationUtil.instantiate(aggWithName.getAggregator(), Aggregator.class);
			aggregators.put(aggWithName.getName(), agg);
		}
		
		// instantiate the aggregator convergence criterion
		if (taskConfig.usesConvergenceCriterion()) {
			Class<? extends ConvergenceCriterion<Value>> convClass = taskConfig.getConvergenceCriterion();
			convergenceCriterion = InstantiationUtil.instantiate(convClass, ConvergenceCriterion.class);
			convergenceAggregatorName = taskConfig.getConvergenceCriterionAggregatorName();
			Preconditions.checkNotNull(convergenceAggregatorName);
		}
		
		maxNumberOfIterations = taskConfig.getNumberOfIterations();
		
		// set up the event handler
		int numEventsTillEndOfSuperstep = taskConfig.getNumberOfEventsUntilInterruptInIterativeGate(0);
		eventHandler = new SyncEventHandler(numEventsTillEndOfSuperstep, aggregators, userCodeClassLoader);
		headEventReader.subscribeToEvent(eventHandler, WorkerDoneEvent.class);

		IntegerRecord dummy = new IntegerRecord();
		
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

	private void readHeadEventChannel(IntegerRecord rec) throws IOException {
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

	private void sendToAllWorkers(AbstractTaskEvent event) throws IOException, InterruptedException {
		headEventReader.publishEvent(event);
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
