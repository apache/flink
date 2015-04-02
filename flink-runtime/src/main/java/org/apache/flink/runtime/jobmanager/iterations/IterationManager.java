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

package org.apache.flink.runtime.jobmanager.iterations;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.ConvergenceCriterion;
import org.apache.flink.runtime.accumulators.AccumulatorEvent;
import org.apache.flink.runtime.jobmanager.accumulators.AccumulatorManager;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.InstantiationUtil;

import akka.actor.ActorRef;

import com.google.common.base.Preconditions;

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
	
	private final ActorRef jobManager;
	 
	private CopyOnWriteArrayList<ActorRef> workers = new CopyOnWriteArrayList<ActorRef>();
	
	public IterationManager(JobID jobId, int iterationId, int numberOfEventsUntilEndOfSuperstep, int maxNumberOfIterations, 
			AccumulatorManager accumulatorManager, ActorRef jobManager) throws IOException {
		Preconditions.checkArgument(numberOfEventsUntilEndOfSuperstep > 0);
		this.jobId = jobId;
		this.iterationId = iterationId;
		this.numberOfEventsUntilEndOfSuperstep = numberOfEventsUntilEndOfSuperstep;
		this.maxNumberOfIterations = maxNumberOfIterations;
		this.accumulatorManager = accumulatorManager;
		this.iterationAccumulators = new HashSet<String>();
		this.jobManager = jobManager;
	}
	
	/**
	 * Is called once the JobManager receives a WorkerDoneEvent by RPC call from one node
	 */
	public synchronized void receiveWorkerDoneEvent(Map<String, Accumulator<?, ?>> accumulators, ActorRef sender) {
		
		// sanity check
		if (this.endOfSuperstep) {
			throw new RuntimeException("Encountered WorderDoneEvent when still in End-of-Superstep status.");
		}
		
		workerDoneEventCounter++;
		
		// process accumulators
		this.accumulatorManager.processIncomingAccumulators(jobId, accumulators);
		
		// add accumulators to managed set to have a notion which accumulators belong to this iteration
		for(String accumulatorName : accumulators.keySet()) {
			this.iterationAccumulators.add(accumulatorName);
		}
		
		// add all senders
		if(this.workers.size() < numberOfEventsUntilEndOfSuperstep) {
			this.workers.add(sender);
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
			for(ActorRef worker : this.workers) {
				worker.tell(new JobManagerMessages.InitIterationTermination(), this.jobManager);
			}

		} else {

			if (log.isInfoEnabled()) {
				log.info("signaling that all workers are done in iteration [" + currentIteration+ "]");
			}

			// important for sanity checking
			resetEndOfSuperstep();
			
			// copy Accumulators for sending to reset locals correctly
			AccumulatorEvent accEvent = new AccumulatorEvent(this.jobId, getManagedAccumulators());
			final AccumulatorEvent copiedEvent;
			
			try {
				copiedEvent = (AccumulatorEvent) InstantiationUtil.createCopy(accEvent);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Could not create copy of AccumulatorEvent for next superstep");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Could not create copy of AccumulatorEvent for next superstep");
			}
			
			
			// initiate next iteration for all workers
			for(ActorRef worker : this.workers) {
				worker.tell(new JobManagerMessages.InitNextIteration(copiedEvent), this.jobManager);
			}
			
			currentIteration++;

			resetManagedAccumulators();
		}
		
		workers.clear();
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