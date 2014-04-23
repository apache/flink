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

package eu.stratosphere.compiler.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.api.common.distributions.DataDistribution;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.util.Visitor;

/**
 * The Optimizer representation of a data sink.
 */
public class DataSinkNode extends OptimizerNode {
	
	protected PactConnection input;			// The input edge
	
	/**
	 * Creates a new DataSinkNode for the given sink operator.
	 * 
	 * @param sink The data sink contract object.
	 */
	public DataSinkNode(GenericDataSink sink) {
		super(sink);
	}

	// --------------------------------------------------------------------------------------
	
	/**
	 * Gets the input of the sink.
	 * 
	 * @return The input connection.
	 */
	public PactConnection getInputConnection() {
		return this.input;
	}
	
	/**
	 * 
	 */
	public OptimizerNode getPredecessorNode() {
		if(this.input != null) {
			return input.getSource();
		} else {
			return null;
		}
	}

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericDataSink getPactContract() {
		return (GenericDataSink) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Data Sink";
	}

	@Override
	public boolean isMemoryConsumer() {
		return getPactContract().getPartitionOrdering() != null || getPactContract().getLocalOrder() != null;
	}

	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.singletonList(this.input);
	}

	public List<PactConnection> getOutgoingConnections() {
		return Collections.emptyList();
	}

	@Override
	public void setInput(Map<Operator, OptimizerNode> contractToNode) {
		Operator children = getPactContract().getInput();

		final OptimizerNode pred;
		final PactConnection conn;
		
		pred = contractToNode.get(children);
		conn = new PactConnection(pred, this);
			
		// create the connection and add it
		this.input = conn;
		pred.addOutgoingConnection(conn);
	}

	/**
	 * Computes the estimated outputs for the data sink. Since the sink does not modify anything, it simply
	 * copies the output estimates from its direct predecessor.
	 */
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
	}

	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		final InterestingProperties iProps = new InterestingProperties();
		
		{
			final Ordering partitioning = getPactContract().getPartitionOrdering();
			final DataDistribution dataDist = getPactContract().getDataDistribution();
			final RequestedGlobalProperties partitioningProps = new RequestedGlobalProperties();
			if (partitioning != null) {
				if(dataDist != null) {
					partitioningProps.setRangePartitioned(partitioning, dataDist);
				} else {
					partitioningProps.setRangePartitioned(partitioning);
				}
				iProps.addGlobalProperties(partitioningProps);
			}
			iProps.addGlobalProperties(partitioningProps);
		}
		
		{
			final Ordering localOrder = getPactContract().getLocalOrder();
			final RequestedLocalProperties orderProps = new RequestedLocalProperties();
			if (localOrder != null) {
				orderProps.setOrdering(localOrder);
			}
			iProps.addLocalProperties(orderProps);
		}
		
		this.input.setInterestingProperties(iProps);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Branch Handling
	// --------------------------------------------------------------------------------------------

	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		addClosedBranches(getPredecessorNode().closedBranchingNodes);
		this.openBranches = getPredecessorNode().getBranchesForParent(this.input);
	}
	
	@Override
	protected List<UnclosedBranchDescriptor> getBranchesForParent(PactConnection parent) {
		// return our own stack of open branches, because nothing is added
		return this.openBranches;
	}

	// --------------------------------------------------------------------------------------------
	//                                   Recursive Optimization
	// --------------------------------------------------------------------------------------------
	
	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}
		
		// calculate alternative sub-plans for predecessor
		List<? extends PlanNode> subPlans = getPredecessorNode().getAlternativePlans(estimator);
		List<PlanNode> outputPlans = new ArrayList<PlanNode>();
		
		final int dop = getDegreeOfParallelism();
		final int subPerInstance = getSubtasksPerInstance();
		final int inDop = getPredecessorNode().getDegreeOfParallelism();
		final int inSubPerInstance = getPredecessorNode().getSubtasksPerInstance();
		final int numInstances = dop / subPerInstance + (dop % subPerInstance == 0 ? 0 : 1);
		final int inNumInstances = inDop / inSubPerInstance + (inDop % inSubPerInstance == 0 ? 0 : 1);
		
		final boolean globalDopChange = numInstances != inNumInstances;
		final boolean localDopChange = numInstances == inNumInstances & subPerInstance != inSubPerInstance;
		
		InterestingProperties ips = this.input.getInterestingProperties();
		for (PlanNode p : subPlans) {
			for (RequestedGlobalProperties gp : ips.getGlobalProperties()) {
				for (RequestedLocalProperties lp : ips.getLocalProperties()) {
					Channel c = new Channel(p);
					gp.parameterizeChannel(c, globalDopChange, localDopChange);

					if (lp.isMetBy(c.getLocalPropertiesAfterShippingOnly())) {
						c.setLocalStrategy(LocalStrategy.NONE);
					} else {
						lp.parameterizeChannel(c);
					}
					
					// no need to check whether the created properties meet what we need in case
					// of ordering or global ordering, because the only interesting properties we have
					// are what we require
					outputPlans.add(new SinkPlanNode(this, "DataSink("+this.getPactContract().getName()+")" ,c));
				}
			}
		}
		
		// cost and prune the plans
		for (PlanNode node : outputPlans) {
			estimator.costOperator(node);
		}
		prunePlanAlternatives(outputPlans);

		this.cachedPlans = outputPlans;
		return outputPlans;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                   Function Annotation Handling
	// --------------------------------------------------------------------------------------------
	
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
		
	// --------------------------------------------------------------------------------------------
	//                                     Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			if (getPredecessorNode() != null) {
				getPredecessorNode().accept(visitor);
			} else {
				throw new CompilerException();
			}
			visitor.postVisit(this);
		}
	}
}
