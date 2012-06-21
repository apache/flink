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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a data sink.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DataSinkNode extends OptimizerNode {
	
	protected PactConnection input = null; // The input edges

	/**
	 * Creates a new DataSinkNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data sink contract object.
	 */
	public DataSinkNode(GenericDataSink pactContract) {
		super(pactContract);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Copy constructor to create a copy of a DataSinkNode with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred
	 *        The new predecessor.
	 * @param conn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected DataSinkNode(DataSinkNode template, OptimizerNode pred, PactConnection conn,
			GlobalProperties globalProps, LocalProperties localProps) {
		super(template, globalProps, localProps);

		this.input = new PactConnection(conn, pred, this);

		// copy the child's branch-plan map
		if(pred.branchPlan != null && pred.branchPlan.size() > 0)
			this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>(pred.branchPlan);
		else 
			this.branchPlan = null;
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @return The input connection.
	 */
	public PactConnection getInConn() {
		return this.input;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @param conn
	 *        The input connection to set.
	 */
	public void setInputConnection(PactConnection conn) {
		this.input = conn;
	}
	
	/**
	 * TODO
	 */
	public OptimizerNode getPredNode() {
		if(this.input != null) {
			return input.getSourcePact();
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case SORT:          return 1;
			case NONE:          return 0;
			default:	        return 0;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.singletonList(this.input);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		List<Contract> children = getPactContract().getInputs();

		for(Contract child : children) {
			OptimizerNode pred = contractToNode.get(child);
	
			PactConnection conn = new PactConnection(pred, this);
			setInputConnection(conn);
			pred.addOutConn(conn);
		}
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

		this.estimatedCardinality = new HashMap<FieldSet, Long>();
		this.estimatedNumRecords = 0;
		this.estimatedOutputSize = 0;

		// we copy the output estimates from the input
		if (this.getPredNode() != null) {
			
			this.estimatedCardinality.putAll(this.getPredNode().getEstimatedCardinalities());
			this.estimatedNumRecords = this.getPredNode().getEstimatedNumRecords();
			this.estimatedOutputSize = this.getPredNode().getEstimatedOutputSize();
		}

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// interesting properties that a data sink may generate are
		// 1) an interest in globally sorted data
		// 2) an interest in range-partitioned data
		// 3) an interest in locally sorted data
		Ordering o = getPactContract().getGlobalOrder();
		if (o != null) {
			InterestingProperties i1 = new InterestingProperties();
			i1.getGlobalProperties().setOrdering(o);

			// costs are a range partitioning and a local sort
			estimator.getRangePartitionCost(this.input, i1.getMaximalCosts());
			Costs c = new Costs();
			estimator.getLocalSortCost(this, this.input, c);
			i1.getMaximalCosts().addCosts(c);

			InterestingProperties i2 = new InterestingProperties();
			
			i2.getGlobalProperties().setPartitioning(PartitionProperty.RANGE_PARTITIONED, o.getInvolvedFields());
			estimator.getRangePartitionCost(this.input, i2.getMaximalCosts());

			this.input.addInterestingProperties(i1);
			this.input.addInterestingProperties(i2);
			
		} else if (getPactContract().getLocalOrder() != null) {
			InterestingProperties i = new InterestingProperties();
			i.getLocalProperties().setOrdering(getPactContract().getLocalOrder());
			estimator.getLocalSortCost(this, this.input, i.getMaximalCosts());
			
			this.input.addInterestingProperties(i);
			
		} else {
			this.input.setNoInterestingProperties();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		List<UnclosedBranchDescriptor> result = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge is necessary
		result = mergeLists(result, this.getPredNode().getBranchesForParent(this));

		this.openBranches = result;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getBranchesForParent(eu.stratosphere.pact.compiler.plan.OptimizerNode)
	 */
	@Override
	protected List<UnclosedBranchDescriptor> getBranchesForParent(OptimizerNode parent)
	{
		// return our own stack of open branches, because nothing is added
		return this.openBranches;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans()
	 */
	@Override
	public List<OptimizerNode> getAlternativePlans(CostEstimator estimator) {
		// the alternative plans are the ones that we have incoming, plus the attached output node
		List<OptimizerNode> outputPlans = new ArrayList<OptimizerNode>();

		// step down to all producer nodes and calculate alternative plans
		List<? extends OptimizerNode> subPlans = this.getPredNode().getAlternativePlans(estimator);

		// build all possible alternative plans for this node
		for(OptimizerNode subPlan : subPlans) {
			
			// check, whether the children have the same
			// sub-plan in the common part before the branches
			// TODO check if this is necessary
			/*
			if (!areBranchCompatible(predList, null)) {
				continue;
			}
			*/

			Ordering go = getPactContract().getGlobalOrder();
			Ordering lo = getPactContract().getLocalOrder();

			GlobalProperties gp;
			LocalProperties lp;

			gp = subPlan.getGlobalProperties().createCopy();
			lp = subPlan.getLocalProperties().createCopy();

			ShipStrategy ss = null;
			LocalStrategy ls = null;

			if (go != null && lo != null) {
				// requires global sort

				ShipStrategy s = this.input.getShipStrategy();
				if (s == ShipStrategy.NONE || s == ShipStrategy.PARTITION_RANGE) {
					// strategy not fixed a priori, or strategy fixed, but valid
					ss = ShipStrategy.PARTITION_RANGE;
				} else {
					// strategy is set a priory --> via compiler hint
					// this input plan cannot produce a valid plan
					continue;
				}

				if (this.localStrategy == LocalStrategy.NONE || this.localStrategy == LocalStrategy.SORT) {
					// strategy not fixed a priori, or strategy fixed, but valid
					ls = LocalStrategy.SORT;
				} else {
					// strategy is set a priory --> via compiler hint
					// this input plan cannot produce a valid plan
					continue;
				}
				
				gp.setPartitioning(PartitionProperty.RANGE_PARTITIONED, go.getInvolvedFields());
				gp.setOrdering(go);
				lp.setOrdering(go);
			} else if (lo != null && lo.isMetBy(lp.getOrdering())) {

				// requires local sort
				if (this.localStrategy == LocalStrategy.NONE || this.localStrategy == LocalStrategy.SORT) {
					// strategy not fixed a priori, or strategy fixed, but valid
					ls = LocalStrategy.SORT;
				} else {
					// strategy is set a priory --> via compiler hint
					// this input plan cannot produce a valid plan
					continue;
				}

				ls = LocalStrategy.SORT;
				lp.setOrdering(lo);
			}

			// check, if a shipping strategy applies
			if (ss == null) {
				ss = ShipStrategy.FORWARD;
			}
			
			DataSinkNode ns = new DataSinkNode(this, subPlan, this.input, gp, lp);
			ns.input.setShipStrategy(ss);

			// check, if a local strategy is necessary
			if (ls == null) {
				ls = LocalStrategy.NONE;
			}
			ns.setLocalStrategy(ls);

			// set the costs
			estimator.costOperator(ns);

			// add the plan
			outputPlans.add(ns);
		}
		
		// prune the plans
		prunePlanAlternatives(outputPlans);

		// check if the list does not contain any plan. That may happen, if the channels specify
		// incompatible shipping strategies.
		if (outputPlans.isEmpty()) {
			throw new CompilerException("Could not create a valid plan for the DataSink contract '"
				+ getPactContract().getName() + "'. The compiler hints specified incompatible shipping strategies.");
		}

		return outputPlans;
	}
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		boolean descend = visitor.preVisit(this);

		if (descend) {
			if(this.getPredNode() != null) {
				this.getPredNode().accept(visitor);
			}

			visitor.postVisit(this);
		}
	}

	public boolean isFieldKept(int input, int fieldNumber) {
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		// DO NOTHING
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getReadSet(int)
	 */
	@Override
	public FieldSet getConstantSet(int input) {
		return null;
	}

	
}
