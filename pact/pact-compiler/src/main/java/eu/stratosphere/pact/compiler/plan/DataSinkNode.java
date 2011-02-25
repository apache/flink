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

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.plan.Visitor;
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
	protected PactConnection input; // The input edge

	/**
	 * Creates a new DataSinkNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data sink contract object.
	 */
	public DataSinkNode(DataSinkContract<?, ?> pactContract) {
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
		if (this.branchPlan == null) {
			this.branchPlan = pred.branchPlan;
		} else {
			this.branchPlan.putAll(pred.branchPlan);
		}
	}

	/**
	 * Gets the fully qualified path to the output file.
	 * 
	 * @return The path to the output file.
	 */
	public String getFilePath() {
		return getPactContract().getFilePath();
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @return The input connection.
	 */
	public PactConnection getInputConnection() {
		return input;
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
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	public DataSinkContract<?, ?> getPactContract() {
		return (DataSinkContract<?, ?>) super.getPactContract();
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
	public boolean isMemoryConsumer() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return Collections.<PactConnection> singletonList(input);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		Contract child = ((DataSinkContract<?, ?>) getPactContract()).getInput();

		OptimizerNode pred = contractToNode.get(child);

		PactConnection conn = new PactConnection(pred, this);
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
		OptimizerNode pred = input == null ? null : input.getSourcePact();

		if (pred != null) {
			this.estimatedKeyCardinality = pred.estimatedKeyCardinality;
			this.estimatedNumRecords = pred.estimatedNumRecords;
			this.estimatedOutputSize = pred.estimatedOutputSize;
		} else {
			this.estimatedKeyCardinality = -1;
			this.estimatedNumRecords = -1;
			this.estimatedOutputSize = -1;
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

		Order o = getPactContract().getGlobalOrder();
		if (o != Order.NONE) {
			InterestingProperties i1 = new InterestingProperties();
			i1.getGlobalProperties().setKeyOrder(o);

			// costs are a range partitioning and a local sort
			estimator.getRangePartitionCost(this, input.getSourcePact(), i1.getMaximalCosts());
			Costs c = new Costs();
			estimator.getLocalSortCost(this, input.getSourcePact(), c);
			i1.getMaximalCosts().addCosts(c);

			InterestingProperties i2 = new InterestingProperties();
			i2.getGlobalProperties().setPartitioning(PartitionProperty.RANGE_PARTITIONED);
			estimator.getRangePartitionCost(this, input.getSourcePact(), i2.getMaximalCosts());

			input.addInterestingProperties(i1);
			input.addInterestingProperties(i2);
		} else if (getPactContract().getLocalOrder() != Order.NONE) {
			InterestingProperties i = new InterestingProperties();
			i.getLocalProperties().setKeyOrder(getPactContract().getLocalOrder());
			estimator.getLocalSortCost(this, input.getSourcePact(), i.getMaximalCosts());
			input.addInterestingProperties(i);
		} else {
			input.setNoInterestingProperties();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches == null) {
			// we don't join branches, so we merely have to check, whether out immediate child is a
			// branch (has multiple outputs). If yes, we add that one as a branch, otherwise out
			// branch stack is the same as the child's
			this.openBranches = input.getSourcePact().getBranchesForParent(this);
		}
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
	public List<DataSinkNode> getAlternativePlans(CostEstimator estimator) {
		// the alternative plans are the ones that we have incoming, plus the attached output node
		List<? extends OptimizerNode> inPlans = input.getSourcePact().getAlternativePlans(estimator);
		List<DataSinkNode> plans = new ArrayList<DataSinkNode>(inPlans.size());

		for (OptimizerNode pred : inPlans) {
			Order go = getPactContract().getGlobalOrder();
			Order lo = getPactContract().getLocalOrder();

			GlobalProperties gp = pred.getGlobalProperties().createCopy();
			LocalProperties lp = pred.getLocalProperties().createCopy();

			ShipStrategy ss = null;
			LocalStrategy ls = null;

			if (go != Order.NONE && go != gp.getKeyOrder()) {
				// requires global sort

				if (input.getShipStrategy() == ShipStrategy.NONE
					|| input.getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
					// strategy not fixed a priori, or strategy fixed, but valid
					ss = ShipStrategy.PARTITION_RANGE;
				} else {
					// strategy is set a priory --> via compiler hint
					// this input plan cannot produce a valid plan
					continue;
				}

				if (localStrategy == LocalStrategy.NONE || localStrategy == LocalStrategy.SORT) {
					// strategy not fixed a priori, or strategy fixed, but valid
					ls = LocalStrategy.SORT;
				} else {
					// strategy is set a priory --> via compiler hint
					// this input plan cannot produce a valid plan
					continue;
				}

				gp.setPartitioning(PartitionProperty.RANGE_PARTITIONED);
				gp.setKeyOrder(go);
				lp.setKeyOrder(go);
			} else if (lo != Order.NONE && lo != lp.getKeyOrder()) {

				// requires local sort
				if (localStrategy == LocalStrategy.NONE || localStrategy == LocalStrategy.SORT) {
					// strategy not fixed a priori, or strategy fixed, but valid
					ls = LocalStrategy.SORT;
				} else {
					// strategy is set a priory --> via compiler hint
					// this input plan cannot produce a valid plan
					continue;
				}

				ls = LocalStrategy.SORT;
				lp.setKeyOrder(lo);
			}

			// create the new node and connection
			DataSinkNode ns = new DataSinkNode(this, pred, input, gp, lp);

			// check, if a shipping strategy applies
			if (ss == null) {
				ss = ShipStrategy.FORWARD;
			}
			ns.input.setShipStrategy(ss);

			// check, if a local strategy is necessary
			if (ls == null) {
				ls = LocalStrategy.NONE;
			}
			ns.setLocalStrategy(ls);

			// set the costs
			estimator.costOperator(ns);

			// add the plan
			plans.add(ns);
		}

		// prune the plans
		prunePlanAlternatives(plans);

		// check if the list does not contain any plan. That may happen, if the channels specify
		// incompatible shipping strategies.
		if (plans.isEmpty()) {
			throw new CompilerException("Could not create a valid plan for the DataSource contract '"
				+ getPactContract().getName() + "'. The compiler hints specified incompatible shipping strategies.");
		}

		return plans;
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
			if (input != null) {
				input.getSourcePact().accept(visitor);
			}

			visitor.postVisit(this);
		}
	}
}
