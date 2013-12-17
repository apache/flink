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

package eu.stratosphere.compiler.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.api.operators.Operator;
import eu.stratosphere.api.operators.util.FieldSet;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.costs.Costs;
import eu.stratosphere.compiler.dag.OptimizerNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.plandump.DumpableConnection;
import eu.stratosphere.compiler.plandump.DumpableNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitable;

/**
 * The representation of a data exchange between to operators. The data exchange can realize a shipping strategy, 
 * which established global properties, and a local strategy, which establishes local properties.
 * <p>
 * Because we currently deal only with plans where the operator order is fixed, many properties are equal
 * among candidates and are determined prior to the enumeration (such as for example constant/dynamic path membership).
 * Hence, many methods will delegate to the {@code OptimizerNode} that represents the node this candidate was
 * created for.
 */
public abstract class PlanNode implements Visitable<PlanNode>, DumpableNode<PlanNode> {
	
	protected final OptimizerNode template;
	
	protected final List<Channel> outChannels;
	
	private final String nodeName; 
	
	private final DriverStrategy driverStrategy;	// The local strategy (sorting / hashing, ...)
	
	protected LocalProperties localProps; 			// local properties of the data produced by this node

	protected GlobalProperties globalProps;			// global properties of the data produced by this node
	
	protected Map<OptimizerNode, PlanNode> branchPlan; // the actual plan alternative chosen at a branch point
	
	protected Costs nodeCosts;						// the costs incurred by this node

	protected Costs cumulativeCosts;					// the cumulative costs of all operators in the sub-tree
	
	private long memoryPerSubTask;					// the amount of memory dedicated to each task, in bytes
	
	private boolean pFlag;							// flag for the internal pruning algorithm
	
	// --------------------------------------------------------------------------------------------
	
	public PlanNode(OptimizerNode template, String nodeName, DriverStrategy strategy) {
		this.outChannels = new ArrayList<Channel>(2);
		this.template = template;
		this.nodeName = nodeName;
		this.driverStrategy = strategy;
		
		// check, if there is branch at this node. if yes, this candidate must be associated with
		// the branching template node.
		if (template.isBranching()) {
			this.branchPlan = new HashMap<OptimizerNode, PlanNode>(6);
			this.branchPlan.put(template, this);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                           Accessors
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the optimizer's pact node for which this plan candidate node was created.
	 * 
	 * @return The template optimizer's node.
	 */
	public OptimizerNode getOriginalOptimizerNode() {
		return this.template;
	}
	
	/**
	 * Gets the pact contract this node represents in the plan.
	 * 
	 * @return The pact contract this node represents in the plan.
	 */
	public Operator getPactContract() {
		return this.template.getPactContract();
	}
	
	/**
	 * Gets the name of the plan node.
	 * 
	 * @return The name of the plan node.
	 */
	public String getNodeName() {
		return this.nodeName;
	}
	
	public int getMemoryConsumerWeight() {
		return this.driverStrategy.isMaterializing() ? 1 : 0;
	}
	
	/**
	 * Gets the memory dedicated to each sub-task for this node.
	 * 
	 * @return The memory per task, in bytes.
	 */
	public long getMemoryPerSubTask() {
		return this.memoryPerSubTask;
	}

	/**
	 * Sets the memory dedicated to each task for this node.
	 * 
	 * @param memoryPerTask The memory per sub-task, in bytes.
	 */
	public void setMemoryPerSubTask(long memoryPerTask) {
		this.memoryPerSubTask = memoryPerTask;
	}
	
	/**
	 * Gets the driver strategy from this node. This determines for example for a <i>match</i> Pact whether
	 * to use a merge or a hybrid hash strategy.
	 * 
	 * @return The driver strategy.
	 */
	public DriverStrategy getDriverStrategy() {
		return this.driverStrategy;
	}
	
	public void initProperties(GlobalProperties globals, LocalProperties locals) {
		if (this.globalProps != null || this.localProps != null) {
			throw new IllegalStateException();
		}
		this.globalProps = globals;
		this.localProps = locals;
	}
	
	/**
	 * Gets the local properties from this PlanNode.
	 *
	 * @return The local properties.
	 */
	public LocalProperties getLocalProperties() {
		return this.localProps;
	}
	
	/**
	 * Gets the global properties from this PlanNode.
	 *
	 * @return The global properties.
	 */
	public GlobalProperties getGlobalProperties() {
		return this.globalProps;
	}
	
	/**
	 * Gets the costs incurred by this node. The costs reflect also the costs incurred by the shipping strategies
	 * of the incoming connections.
	 * 
	 * @return The node-costs, or null, if not yet set.
	 */
	public Costs getNodeCosts() {
		return this.nodeCosts;
	}

	/**
	 * Gets the cumulative costs of this nose. The cumulative costs are the the sum of the costs
	 * of this node and of all nodes in the subtree below this node.
	 * 
	 * @return The cumulative costs, or null, if not yet set.
	 */
	public Costs getCumulativeCosts() {
		return this.cumulativeCosts;
	}
	
	/**
	 * Sets the basic cost for this {@code OptimizerNode}
	 * 
	 * @param nodeCosts		The already knows costs for this node
	 * 						(this cost a produces by a concrete {@code OptimizerNode} subclass.
	 */
	public void setCosts(Costs nodeCosts) {
		// set the node costs
		this.nodeCosts = nodeCosts;
		// the cumulative costs are the node costs plus the costs of all inputs
		this.cumulativeCosts = nodeCosts.clone();
		for (Iterator<PlanNode> preds = getPredecessors(); preds.hasNext();) {
			Costs parentCosts = preds.next().cumulativeCosts;
			if (parentCosts != null)
				this.cumulativeCosts.addCosts(parentCosts);
			else
				throw new CompilerException();
		}
	}
	
	public int getDegreeOfParallelism() {
		return this.template.getDegreeOfParallelism();
	}
	
	public int getSubtasksPerInstance() {
		return this.template.getSubtasksPerInstance();
	}
	
	public long getGuaranteedAvailableMemory() {
		return this.template.getMinimalMemoryAcrossAllSubTasks();
	}

	// --------------------------------------------------------------------------------------------
	//                               Input, Predecessors, Successors
	// --------------------------------------------------------------------------------------------
	
	public abstract Iterator<Channel> getInputs();
	
	public abstract Iterator<PlanNode> getPredecessors();
	
	/**
	 * Adds a channel to a successor node to this node.
	 * 
	 * @param channel The channel to the successor.
	 */
	public void addOutgoingChannel(Channel channel) {
		this.outChannels.add(channel);
	}
	
	/**
	 * Gets a list of all outgoing channels leading to successors.
	 * 
	 * @return A list of all channels leading to successors.
	 */
	public List<Channel> getOutgoingChannels() {
		return this.outChannels;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//                                Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	public void updatePropertiesWithUniqueSets(Set<FieldSet> uniqueFieldCombinations) {
		if (uniqueFieldCombinations == null || uniqueFieldCombinations.isEmpty()) {
			return;
		}
		for (FieldSet fields : uniqueFieldCombinations) {
			this.globalProps.addUniqueFieldCombination(fields);
			this.localProps.addUniqueFields(fields);
		}
	}
	
	public PlanNode getCandidateAtBranchPoint(OptimizerNode branchPoint) {
		return this.branchPlan.get(branchPoint);
	}
	
	/**
	 * Sets the pruning marker to true.
	 */
	public void setPruningMarker() {
		this.pFlag = true;
	}
	
	/**
	 * Checks whether the pruning marker was set.
	 * 
	 * @return True, if the pruning marker was set, false otherwise.
	 */
	public boolean isPruneMarkerSet() {
		return this.pFlag;
	}
	
	public boolean isOnDynamicPath() {
		return this.template.isOnDynamicPath();
	}
	
	public int getCostWeight() {
		return this.template.getCostWeight();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public abstract SourceAndDamReport hasDamOnPathDownTo(PlanNode source);
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.template.getName() + " \"" + getPactContract().getName() + "\" : " + this.driverStrategy +
				" [[ " + this.globalProps + " ]] [[ " + this.localProps + " ]]";
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plandump.DumpableNode#getOptimizerNode()
	 */
	@Override
	public OptimizerNode getOptimizerNode() {
		return this.template;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plandump.DumpableNode#getPlanNode()
	 */
	@Override
	public PlanNode getPlanNode() {
		return this;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plandump.DumpableNode#getDumpableInputs()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Iterator<DumpableConnection<PlanNode>> getDumpableInputs() {
		return (Iterator<DumpableConnection<PlanNode>>) (Iterator<?>) getInputs();
	}
	
	public static enum SourceAndDamReport {
		NOT_FOUND, FOUND_SOURCE, FOUND_SOURCE_AND_DAM;
	}
}
