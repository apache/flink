/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.checkpointing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.util.SerializableArrayList;

/**
 * The checkpoint decision coordinator is the central instance to make a coordinated checkpoint decision. Incoming jobs
 * are registered with this decision coordinator. As a result, the decision coordinator follows the resource exhaustion
 * of the tasks included in the registered job and eventually decides if it is beneficial to materialized
 * {@link EphemeralCheckpoint} objects or to discard them.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class CheckpointDecisionCoordinator {

	/**
	 * The log object used to report errors and information in general.
	 */
	private static final Log LOG = LogFactory.getLog(CheckpointDecisionCoordinator.class);

	/**
	 * The object in charge of propagating checkpoint decisions to the respective task managers.
	 */
	private final CheckpointDecisionPropagator decisionPropagator;

	/**
	 * Constructs a new checkpoint decision coordinator.
	 * 
	 * @param decisionPropagator
	 *        the object in charge of propagating the checkpoint decisions to the respective task managers.
	 */
	public CheckpointDecisionCoordinator(final CheckpointDecisionPropagator decisionPropagator) {
		this.decisionPropagator = decisionPropagator;
	}

	/**
	 * Registers a new {@link ExecutionGraph} with the checkpoint decision coordinator. As a result of this operation,
	 * the checkpoint decision coordinator will receive events about resource exhaustion of the tasks included in this
	 * job.
	 * 
	 * @param executionGraph
	 *        the job to register
	 */
	public void registerJob(final ExecutionGraph executionGraph) {

		synchronized (executionGraph) {

			final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(executionGraph, true);
			while (it.hasNext()) {
				final ExecutionVertex vertex = it.next();
				vertex.registerExecutionListener(new CheckpointExecutionListener(this, vertex));
			}
		}
	}

	/**
	 * Computes a checkpoint decision for the given {@link ExecutionVertex}.
	 * 
	 * @param vertex
	 *        the vertex which requires a checkpoint decision
	 * @param rus
	 *        the current resource utilization of the vertex
	 */
	void checkpointDecisionRequired(final ExecutionVertex vertex, final ResourceUtilizationSnapshot rus) {
		boolean checkpointDesicion = false;
		LOG.info("Checkpoint decision for vertex " + vertex + " required");
		// This implementation always creates the checkpoint
		
		// TODO: Provide sensible implementation here

		if(rus.getUserCPU() >= 90){
			LOG.info("CPU-Bottleneck");
			//CPU bottleneck 
			checkpointDesicion = true;
		}else{
			Environment ee = vertex.getEnvironment();
			double in = 0;
			for(int i = 0; i < ee.getNumberOfInputGates(); i++){
				in += ee.getInputGate(i).getNumberOfInputChannels();
			}
			double out = 0;
			for(int i = 0; i < ee.getNumberOfOutputGates(); i++){
				out += ee.getOutputGate(i).getNumberOfOutputChannels();
			}
			if( out != 0 && in/out > 1.5){
				LOG.info("vertex.getNumberOfPredecessors()/vertex.getNumberOfSuccessors() > 1.5");
				//less output-channels than input-channels 
				//checkpoint at this position probably saves network-traffic 
				checkpointDesicion = true;
			}else if(true){
				//always create checkpoint for testing
				checkpointDesicion = true;
			}
		}
		final ExecutionGraph graph = vertex.getExecutionGraph();
		final Map<AbstractInstance, List<CheckpointDecision>> checkpointDecisions = new HashMap<AbstractInstance, List<CheckpointDecision>>();
		final List<CheckpointDecision> checkpointDecisionList = new SerializableArrayList<CheckpointDecision>();

		synchronized (graph) {
			checkpointDecisionList.add(new CheckpointDecision(vertex.getID(), checkpointDesicion));
			checkpointDecisions.put(vertex.getAllocatedResource().getInstance(), checkpointDecisionList);
		}
		
		// Propagate checkpoint decisions
		this.decisionPropagator.propagateCheckpointDecisions(checkpointDecisions);
	}
	
}
