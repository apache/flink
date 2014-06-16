/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.jobmanager.iterations;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.ConvergenceCriterion;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.accumulators.AccumulatorManager;
import eu.stratosphere.nephele.services.accumulators.AccumulatorEvent;
import eu.stratosphere.nephele.taskmanager.runtime.ExecutorThreadFactory;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;
import eu.stratosphere.util.InstantiationUtil;

/**
 * Manages the supersteps of one iteration. The JobManager is holding one IterationManager for every iteration that is 
 * currently running.
 *
 */
public class IterationManager {

	private static final Log log = LogFactory.getLog(IterationManager.class);
	
	private JobID jobId;
	
	private int iterationId;
	
	int numberOfEventsUntilEndOfSuperstep;
	
	int maxNumberOfIterations;
	
	int currentIteration = 1; // count starts at 1, not 0
	
	private int workerDoneEventCounter = 0;
	
	private ConvergenceCriterion<Object> convergenceCriterion;
	
	private String convergenceAccumulatorName;
	
	private boolean endOfSuperstep = false;
	
	private AccumulatorManager accumulatorManager;
	
	private Set<String> iterationAccumulators;
	
	private CopyOnWriteArrayList<ExecutionVertex> executionVertices;
	
	private final ExecutorService executorService = Executors.newCachedThreadPool(ExecutorThreadFactory.INSTANCE);
	
	public IterationManager(JobID jobId, int iterationId, int numberOfEventsUntilEndOfSuperstep, int maxNumberOfIterations, 
			AccumulatorManager accumulatorManager, CopyOnWriteArrayList<ExecutionVertex> executionVertices) throws IOException {
		Preconditions.checkArgument(numberOfEventsUntilEndOfSuperstep > 0);
		this.jobId = jobId;
		this.iterationId = iterationId;
		this.numberOfEventsUntilEndOfSuperstep = numberOfEventsUntilEndOfSuperstep;
		this.maxNumberOfIterations = maxNumberOfIterations;
		this.accumulatorManager = accumulatorManager;
		this.executionVertices = executionVertices;
		this.iterationAccumulators = new HashSet<String>();
	}
	
	/**
	 * Is called once the JobManager receives a WorkerDoneEvent by RPC call from one node
	 */
	public synchronized void receiveWorkerDoneEvent(WorkerDoneEvent workerDoneEvent) {
		
		// sanity check
		if (this.endOfSuperstep) {
			throw new RuntimeException("Encountered WorderDoneEvent when still in End-of-Superstep status.");
		}
		
		workerDoneEventCounter++;
		
		// process accumulators
		this.accumulatorManager.processIncomingAccumulators(workerDoneEvent.getJobId(), workerDoneEvent.getAccumulators());
		
		// add accumulators to managed set to have a notion which accumulators belong to this iteration
		for(String accumulatorName : workerDoneEvent.getAccumulators().keySet()) {
			this.iterationAccumulators.add(accumulatorName);
		}
		
		// if all workers have sent their WorkerDoneEvent -> end of superstep
		if (workerDoneEventCounter % numberOfEventsUntilEndOfSuperstep == 0) {
			endOfSuperstep = true;
			handleEndOfSuperstep();
		}
	}
	
	/**
	 * Handles the end of one superstep. If convergence is reached it sends a termination request to all connected workers.
	 * If not it initializes the next superstep by sending an AllWorkersDoneEvent (with aggregators) to all workers.
	 */
	private void handleEndOfSuperstep() {
		if (log.isInfoEnabled()) {
			log.info("finishing iteration [" + currentIteration + "]");
		}

		if (checkForConvergence()) {

			if (log.isInfoEnabled()) {
				log.info("signaling that all workers are to terminate in iteration ["+ currentIteration + "]");
			}
			
			// Send termination to all workers
			for(ExecutionVertex ev : this.executionVertices) {
				
				final AbstractInstance instance = ev.getAllocatedResource().getInstance();
				if (instance == null) {
					log.error("Could not find instance to sent termination request for iteration.");
					return;
				}
				
				final ExecutionVertexID headVertexId = ev.getID();
				
				// send kill request
				final Runnable runnable = new Runnable() {
					@Override
					public void run() {
						try {
							instance.terminateIteration(headVertexId);
						} catch (IOException ioe) {
							log.error(ioe);
						}
					}
				};
				executorService.execute(runnable);
			}

		} else {

			if (log.isInfoEnabled()) {
				log.info("signaling that all workers are done in iteration [" + currentIteration+ "]");
			}

			// important for sanity checking
			resetEndOfSuperstep();
			
			AllWorkersDoneEvent allWorkersDoneEvent = new AllWorkersDoneEvent(new AccumulatorEvent(this.jobId, getManagedAccumulators(), false));
			final AllWorkersDoneEvent copiedEvent;
			
			try {
				copiedEvent = (AllWorkersDoneEvent) InstantiationUtil.createCopy(allWorkersDoneEvent);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Could not create copy of AllWorkersDoneEvent for next superstep");
			}
			
			
			// Send start of next superstep to all workers
			for(ExecutionVertex ev : this.executionVertices) {
				
				final AbstractInstance instance = ev.getAllocatedResource().getInstance();
				if (instance == null) {
					log.error("Could not find instance to sent termination request for iteration.");
					return;
				}
				
				final ExecutionVertexID headVertexId = ev.getID();
				
				// send kill request
				final Runnable runnable = new Runnable() {
					@Override
					public void run() {
						try {
							instance.startNextSuperstep(headVertexId, copiedEvent);
						} catch (IOException ioe) {
							log.error(ioe);
						}
					}
				};
				executorService.execute(runnable);
			}
			
			currentIteration++;


			resetManagedAccumulators();
		}
	}
	
	public boolean isEndOfSuperstep() {
		return this.endOfSuperstep;
	}
	
	public void resetEndOfSuperstep() {
		this.endOfSuperstep = false;
	}
	
	public void setConvergenceCriterion(String convergenceAccumulatorName, ConvergenceCriterion<Object> convergenceCriterion) {
		this.convergenceAccumulatorName = convergenceAccumulatorName;
		this.convergenceCriterion = convergenceCriterion;
	}
	
	/**
	 * Checks if either we have reached maxNumberOfIterations or if a associated ConvergenceCriterion is converged
	 */
	private boolean checkForConvergence() {
		
		if (maxNumberOfIterations == currentIteration) {
			if (log.isInfoEnabled()) {
				log.info("maximum number of iterations [" + currentIteration+ "] reached, terminating...");
			}
			return true;
		}

		if (convergenceAccumulatorName != null) {

			Accumulator<?, ? extends Object> acc = this.accumulatorManager.getJobAccumulators(jobId).get(convergenceAccumulatorName);

			if (acc == null) {
				throw new RuntimeException("Error: Accumulator for convergence criterion was null.");
			}
			
			Object aggregate = acc.getLocalValue();

			if (convergenceCriterion.isConverged(currentIteration, aggregate)) {
				if (log.isInfoEnabled()) {
					log.info("convergence reached after [" + currentIteration + "] iterations, terminating...");
				}
				return true;
			}
		}
		
		return false;
	}
	
	public JobID getJobId() {
		return jobId;
	}

	public int getIterationId() {
		return iterationId;
	}
	
	/**
	 * Utility method used to reset the managed accumulators of this iteration after a superstep
	 */
	private void resetManagedAccumulators() {
		
		Iterator<Map.Entry<String, Accumulator<?, ?>>> it = this.accumulatorManager.getJobAccumulators(jobId).entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Accumulator<?, ?>> pair = (Map.Entry<String, Accumulator<?, ?>>) it.next();
			
			// is this accumulator managed by this iteration?
			if(this.iterationAccumulators.contains(pair.getKey())) {
				pair.getValue().resetLocal();
			}
		}
	}
	
	/**
	 * Utility method to retrieve a Map of all managed accumulators of this iteration.
	 * Used for the report back to the task managers
	 */
	private Map<String, Accumulator<?, ?>> getManagedAccumulators() {
		
		Map<String, Accumulator<?, ?>> managedAccumulators = new HashMap<String, Accumulator<?, ?>>();
		
		Iterator<Map.Entry<String, Accumulator<?, ?>>> it = this.accumulatorManager.getJobAccumulators(jobId).entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Accumulator<?, ?>> pair = (Map.Entry<String, Accumulator<?, ?>>) it.next();
			
			// is this accumulator managed by this iteration?
			if(this.iterationAccumulators.contains(pair.getKey())) {
				managedAccumulators.put(pair.getKey(), pair.getValue());
			}
		}
		
		return managedAccumulators;
	}
}
