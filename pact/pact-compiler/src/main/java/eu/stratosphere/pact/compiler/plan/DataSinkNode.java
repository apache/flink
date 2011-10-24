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
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
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
	final protected List<PactConnection> input = new ArrayList<PactConnection>(); // The input edge

	/**
	 * Creates a new DataSinkNode for the given contract.
	 * 
	 * @param pactContract
	 *        The data sink contract object.
	 */
	public DataSinkNode(FileDataSinkContract<?, ?> pactContract) {
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
	protected DataSinkNode(DataSinkNode template, List<OptimizerNode> pred, List<PactConnection> conn,
			GlobalProperties globalProps, LocalProperties localProps) {
		super(template, globalProps, localProps);

		int i = 0;
		for(PactConnection c: conn) {
			this.input.add(new PactConnection(c, pred.get(i++), this));
		}

		// copy the child's branch-plan map
		if (this.branchPlan == null) {
			this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>();
		}
		for(OptimizerNode n : pred) {
			if(n.branchPlan != null)
				this.branchPlan.putAll(n.branchPlan);
		}
		if(this.branchPlan.size() == 0)
			this.branchPlan = null;

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
	public List<PactConnection> getInputConnections() {
		return this.input;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its input.
	 * 
	 * @param conn
	 *        The input connection to set.
	 */
	public void addInputConnection(PactConnection conn) {
		this.input.add(conn);
	}

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public FileDataSinkContract<?, ?> getPactContract() {
		return (FileDataSinkContract<?, ?>) super.getPactContract();
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
	public List<List<PactConnection>> getIncomingConnections() {
		return Collections.singletonList(this.input);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		List<Contract> children = ((FileDataSinkContract<?, ?>) getPactContract()).getInputs();

		for(Contract child : children) {
			OptimizerNode pred = contractToNode.get(child);
	
			PactConnection conn = new PactConnection(pred, this);
			addInputConnection(conn);
			pred.addOutgoingConnection(conn);
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
		this.estimatedKeyCardinality = 0;
		this.estimatedNumRecords = 0;
		this.estimatedOutputSize = 0;

		// we copy the output estimates from the input
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();

			if (pred != null) {
				// if one input (all of them are unioned) does not know
				// its key cardinality, we a pessimistic and return "unknown" as well
				if(pred.estimatedKeyCardinality == -1) {
					this.estimatedKeyCardinality = -1;
					break;
				}
				
				this.estimatedKeyCardinality += pred.estimatedKeyCardinality;
			}
		}

		// we copy the output estimates from the input
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();

			if (pred != null) {
				// if one input (all of them are unioned) does not know
				// its record count, we a pessimistic and return "unknown" as well
				if(pred.estimatedNumRecords == -1) {
					this.estimatedNumRecords = -1;
					break;
				}
				
				this.estimatedNumRecords += pred.estimatedNumRecords;
			}
		}
		
		// we copy the output estimates from the input
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();

			if (pred != null) {
				// if one input (all of them are unioned) does not know
				// its output size, we a pessimistic and return "unknown" as well
				if(pred.estimatedOutputSize == -1) {
					this.estimatedOutputSize = -1;
					break;
				}
				
				this.estimatedOutputSize += pred.estimatedOutputSize;
			}
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
			estimator.getRangePartitionCost(this.input, i1.getMaximalCosts());
			Costs c = new Costs();
			estimator.getLocalSortCost(this, this.input, c);
			i1.getMaximalCosts().addCosts(c);

			InterestingProperties i2 = new InterestingProperties();
			i2.getGlobalProperties().setPartitioning(PartitionProperty.RANGE_PARTITIONED);
			estimator.getRangePartitionCost(this.input, i2.getMaximalCosts());

			for(PactConnection pc : this.input) {
				pc.addInterestingProperties(i1);
				pc.addInterestingProperties(i2);
			}
		} else if (getPactContract().getLocalOrder() != Order.NONE) {
			InterestingProperties i = new InterestingProperties();
			i.getLocalProperties().setKeyOrder(getPactContract().getLocalOrder());
			estimator.getLocalSortCost(this, this.input, i.getMaximalCosts());
			for(PactConnection c : this.input)
				c.addInterestingProperties(i);
		} else {
			for(PactConnection c : this.input)
				c.setNoInterestingProperties();
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
			this.openBranches = new ArrayList<UnclosedBranchDescriptor>();
			for(PactConnection c : this.input) {
				List<UnclosedBranchDescriptor> parentBranchList = c.getSourcePact().getBranchesForParent(this);
				if(parentBranchList != null)
					this.openBranches.addAll(parentBranchList);
			}
			if(this.openBranches.size() == 0)
				this.openBranches = null;
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
		List<DataSinkNode> plans = new ArrayList<DataSinkNode>();
		getAlternativePlansRecursively(new ArrayList<OptimizerNode>(0), estimator, plans);

//		for (OptimizerNode pred : inPlans) {
//			Order go = getPactContract().getGlobalOrder();
//			Order lo = getPactContract().getLocalOrder();
//
//			GlobalProperties gp = pred.getGlobalProperties().createCopy();
//			LocalProperties lp = pred.getLocalProperties().createCopy();
//
//			ShipStrategy ss = null;
//			LocalStrategy ls = null;
//
//			if (go != Order.NONE && go != gp.getKeyOrder()) {
//				// requires global sort
//
//				if (this.input.getShipStrategy() == ShipStrategy.NONE
//					|| this.input.getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
//					// strategy not fixed a priori, or strategy fixed, but valid
//					ss = ShipStrategy.PARTITION_RANGE;
//				} else {
//					// strategy is set a priory --> via compiler hint
//					// this input plan cannot produce a valid plan
//					continue;
//				}
//
//				if (this.localStrategy == LocalStrategy.NONE || this.localStrategy == LocalStrategy.SORT) {
//					// strategy not fixed a priori, or strategy fixed, but valid
//					ls = LocalStrategy.SORT;
//				} else {
//					// strategy is set a priory --> via compiler hint
//					// this input plan cannot produce a valid plan
//					continue;
//				}
//
//				gp.setPartitioning(PartitionProperty.RANGE_PARTITIONED);
//				gp.setKeyOrder(go);
//				lp.setKeyOrder(go);
//			} else if (lo != Order.NONE && lo != lp.getKeyOrder()) {
//
//				// requires local sort
//				if (this.localStrategy == LocalStrategy.NONE || this.localStrategy == LocalStrategy.SORT) {
//					// strategy not fixed a priori, or strategy fixed, but valid
//					ls = LocalStrategy.SORT;
//				} else {
//					// strategy is set a priory --> via compiler hint
//					// this input plan cannot produce a valid plan
//					continue;
//				}
//
//				ls = LocalStrategy.SORT;
//				lp.setKeyOrder(lo);
//			}
//
//			// create the new node and connection
//			DataSinkNode ns = new DataSinkNode(this, pred, this.input, gp, lp);
//
//			// check, if a shipping strategy applies
//			if (ss == null) {
//				ss = ShipStrategy.FORWARD;
//			}
//			ns.input.setShipStrategy(ss);
//
//			// check, if a local strategy is necessary
//			if (ls == null) {
//				ls = LocalStrategy.NONE;
//			}
//			ns.setLocalStrategy(ls);
//
//			// set the costs
//			estimator.costOperator(ns);
//
//			// add the plan
//			plans.add(ns);
//		}

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

	private void getAlternativePlansRecursively(List<OptimizerNode> allPreds, CostEstimator estimator, List<DataSinkNode> outputPlans) {
		// what is out recursive depth
		final int allPredsSize = allPreds.size();
		// pick the connection this recursive step has to process
		PactConnection connToProcess = this.input.get(allPredsSize);
		// get all alternatives for current recursion level
		List<? extends OptimizerNode> inPlans = connToProcess.getSourcePact().getAlternativePlans(estimator);
		
		// now enumerate all alternative of this recursion level
		for (OptimizerNode pred : inPlans) {
			// add an alternative plan node
			allPreds.add(pred);
			
			Order go = getPactContract().getGlobalOrder();
			Order lo = getPactContract().getLocalOrder();

			GlobalProperties gp = pred.getGlobalProperties().createCopy();
			LocalProperties lp = pred.getLocalProperties().createCopy();

			ShipStrategy ss = null;
			LocalStrategy ls = null;

			if (go != Order.NONE && go != gp.getKeyOrder()) {
				// requires global sort

				if (connToProcess.getShipStrategy() == ShipStrategy.NONE
					|| connToProcess.getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
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

				gp.setPartitioning(PartitionProperty.RANGE_PARTITIONED);
				gp.setKeyOrder(go);
				lp.setKeyOrder(go);
			} else if (lo != Order.NONE && lo != lp.getKeyOrder()) {

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
				lp.setKeyOrder(lo);
			}
			
			// check if the hit the last recursion level
			if(allPredsSize + 1 == this.input.size()) {
				// last recursion level: create a new alternative now
				
				DataSinkNode ns = new DataSinkNode(this, allPreds, this.input, gp, lp);
				// check, if a shipping strategy applies
				if (ss == null) {
					ss = ShipStrategy.FORWARD;
				}
				for(PactConnection cc : ns.getInputConnections()) {
					cc.setShipStrategy(ss);
				}

				// check, if a local strategy is necessary
				if (ls == null) {
					ls = LocalStrategy.NONE;
				}
				ns.setLocalStrategy(ls);

				// set the costs
				estimator.costOperator(ns);

				// add the plan
				outputPlans.add(ns);

			} else {
				getAlternativePlansRecursively(allPreds, estimator, outputPlans);
			}
			
			// remove the added alternative plan node, in order to replace it with the next alternative at the beginning of the loop
			allPreds.remove(allPredsSize);
		}
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
			for(PactConnection c : this.input) {
				OptimizerNode n = c.getSourcePact();
				if (n != null) {
					n.accept(visitor);
				}
			}

			visitor.postVisit(this);
		}
	}
}
