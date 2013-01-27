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

package eu.stratosphere.pact.compiler.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * The Optimizer representation of a data sink.
 */
public class DataSinkNode extends OptimizerNode
{
	protected PactConnection input;			// The input edge
	
	private List<PlanNode> cachedPlans;		// plan candidate cache

	
	/**
	 * Creates a new DataSinkNode for the given contract.
	 * 
	 * @param pactContract The data sink contract object.
	 */
	public DataSinkNode(GenericDataSink pactContract) {
		super(pactContract);
	}

	// --------------------------------------------------------------------------------------
	
	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @return The input connection.
	 */
	public PactConnection getInputConnection() {
		return this.input;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @param conn The input connection to set.
	 */
	public void setInputConnection(PactConnection conn) {
		this.input = conn;
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Data Sink";
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return getPactContract().getPartitionOrdering() != null || getPactContract().getLocalOrder() != null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.singletonList(this.input);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getOutgoingConnections()
	 */
	public List<PactConnection> getOutgoingConnections() {
		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		List<Contract> children = getPactContract().getInputs();

		final OptimizerNode pred;
		final PactConnection conn;
		if (children.size() == 1) {
			pred = contractToNode.get(children.get(0));
			conn = new PactConnection(pred, this);
		} else {
			pred = createdUnionCascade(children, contractToNode, null);
			conn = new PactConnection(pred, this);
			conn.setShipStrategy(ShipStrategyType.FORWARD);
		}
		// create the connection and add it
		setInputConnection(conn);
		pred.addOutgoingConnection(conn);
	}

	/**
	 * Computes the estimated outputs for the data sink. Since the sink does not modify anything, it simply
	 * copies the output estimates from its direct predecessor. Any compiler hints on the data sink are
	 * ignored.
	 * 
	 * @param statistics
	 *        The statistics wrapper to be used to obtain additional knowledge. Ignored.
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		// we copy the output estimates from the input
		if (this.getPredecessorNode() == null) {
			throw new CompilerException();
		}
		
		if (this.estimatedCardinality.size() > 0)
			this.estimatedCardinality.clear();
		
		this.estimatedCardinality.putAll(getPredecessorNode().getEstimatedCardinalities());
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator)
	{
		final InterestingProperties iProps = new InterestingProperties();
		
		{
			final Ordering partitioning = getPactContract().getPartitionOrdering();
			final RequestedGlobalProperties partitioningProps = new RequestedGlobalProperties();
			if (partitioning != null) {
				partitioningProps.setRangePartitioned(partitioning);
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		addClosedBranches(getPredecessorNode().closedBranchingNodes);
		this.openBranches = getPredecessorNode().getBranchesForParent(this.input);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getBranchesForParent(eu.stratosphere.pact.compiler.plan.OptimizerNode)
	 */
	@Override
	protected List<UnclosedBranchDescriptor> getBranchesForParent(PactConnection parent) {
		// return our own stack of open branches, because nothing is added
		return this.openBranches;
	}

	// --------------------------------------------------------------------------------------------
	//                                   Recursive Optimization
	// --------------------------------------------------------------------------------------------
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator)
	{
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
					outputPlans.add(new SinkPlanNode(this, c));
				}
			}
		}
		
		// cost and prune the plans
		for (PlanNode node : outputPlans) {
			estimator.costOperator(node);
		}
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (isBranching()) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                   Stub Annotation Handling
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldConstant(int, int)
	 */
	public boolean isFieldConstant(int input, int fieldNumber) {
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
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
