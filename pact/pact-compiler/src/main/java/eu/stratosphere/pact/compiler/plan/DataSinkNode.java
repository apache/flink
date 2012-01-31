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
	final protected List<PactConnection> input = new ArrayList<PactConnection>(); // The input edges

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
	public List<List<PactConnection>> getIncomingConnections() {
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
//		this.estimatedKeyCardinality = 0;
		this.estimatedNumRecords = 0;
		this.estimatedOutputSize = 0;

		// we copy the output estimates from the input
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();

			if (pred != null) {
				// if one input (all of them are unioned) does not know
				// its key cardinality, we a pessimistic and return "unknown" as well
//				if(pred.estimatedKeyCardinality == -1) {
//					this.estimatedKeyCardinality = -1;
//					break;
//				}
//				
//				this.estimatedKeyCardinality += pred.estimatedKeyCardinality;
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
			int[] fieldSet = new int[o.getInvolvedIndexes().size()];
			
			for (int i = 0; i < 0; i++) {
				fieldSet[i] = o.getInvolvedIndexes().get(i);
			}
			
			i2.getGlobalProperties().setPartitioning(PartitionProperty.RANGE_PARTITIONED, fieldSet);
			estimator.getRangePartitionCost(this.input, i2.getMaximalCosts());

			for(PactConnection pc : this.input) {
				pc.addInterestingProperties(i1);
				pc.addInterestingProperties(i2);
			}
		} else if (getPactContract().getLocalOrder() != null) {
			InterestingProperties i = new InterestingProperties();
			i.getLocalProperties().setOrdering(getPactContract().getLocalOrder());
			estimator.getLocalSortCost(this, this.input, i.getMaximalCosts());
			for(PactConnection pc : this.input) {
				pc.addInterestingProperties(i);
			}
		} else {
			for(PactConnection pc : this.input) {
				pc.setNoInterestingProperties();
			}
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
		for(PactConnection c : this.input) {
			result = mergeLists(result, c.getSourcePact().getBranchesForParent(this));
		}

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
		final int inputSize = this.input.size();
		@SuppressWarnings("unchecked")
		List<? extends OptimizerNode>[] inPlans = new List[inputSize];
		for(int i = 0; i < inputSize; ++i) {
			inPlans[i] = this.input.get(i).getSourcePact().getAlternativePlans(estimator);
		}

		// build all possible alternative plans for this node
		List<List<OptimizerNode>> alternativeSubPlanCominations = new ArrayList<List<OptimizerNode>>();
		getAlternativeSubPlanCombinationsRecursively(inPlans, new ArrayList<OptimizerNode>(0), alternativeSubPlanCominations);
		
		for(List<OptimizerNode> predList : alternativeSubPlanCominations) {
			
			// check, whether the children have the same
			// sub-plan in the common part before the branches
			if (!areBranchCompatible(predList, null)) {
				continue;
			}

			Ordering go = getPactContract().getGlobalOrder();
			Ordering lo = getPactContract().getLocalOrder();

			GlobalProperties gp;
			LocalProperties lp;

			if(predList.size() == 1) {
				gp = predList.get(0).getGlobalProperties().createCopy();
				lp = predList.get(0).getLocalProperties().createCopy();
			} else {
				// TODO right now we drop all properties in the union case; need to figure out what properties can be kept
				gp = new GlobalProperties();
				lp = new LocalProperties();
			}

			ShipStrategy ss = null;
			LocalStrategy ls = null;

			if (go != null && go != null) {
				// requires global sort

				for(PactConnection c : this.input) {
					ShipStrategy s = c.getShipStrategy();
					if (s == ShipStrategy.NONE || s == ShipStrategy.PARTITION_RANGE) {
						// strategy not fixed a priori, or strategy fixed, but valid
						ss = ShipStrategy.PARTITION_RANGE;
					} else {
						// strategy is set a priory --> via compiler hint
						// this input plan cannot produce a valid plan
						continue;
					}
				}

				if (this.localStrategy == LocalStrategy.NONE || this.localStrategy == LocalStrategy.SORT) {
					// strategy not fixed a priori, or strategy fixed, but valid
					ls = LocalStrategy.SORT;
				} else {
					// strategy is set a priory --> via compiler hint
					// this input plan cannot produce a valid plan
					continue;
				}
				
				int[] fieldSet = new int[go.getInvolvedIndexes().size()];
				
				for (int i = 0; i < 0; i++) {
					fieldSet[i] = go.getInvolvedIndexes().get(i);
				}
				
				
				gp.setPartitioning(PartitionProperty.RANGE_PARTITIONED, fieldSet);
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
			
			DataSinkNode ns = new DataSinkNode(this, predList, this.input, gp, lp);
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
		}
		
		// prune the plans
		prunePlanAlternatives(outputPlans);

		// check if the list does not contain any plan. That may happen, if the channels specify
		// incompatible shipping strategies.
		if (outputPlans.isEmpty()) {
			throw new CompilerException("Could not create a valid plan for the DataSource contract '"
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
			for(PactConnection c : this.input) {
				OptimizerNode n = c.getSourcePact();
				if (n != null) {
					n.accept(visitor);
				}
			}

			visitor.postVisit(this);
		}
	}

	public boolean isFieldKept(int input, int fieldNumber) {
		return false;
	}
	
	@Override
	protected void readCopyProjectionAnnotations() {
		// DO NOTHING		
	}

	@Override
	protected void readReadsAnnotation() {
		// DO NOTHING
	}
	
	@Override
	public void deriveOutputSchema() {
		// DataSink has no output
		// DO NOTHING
	}
	
	@Override
	public int[] computeOutputSchema(List<OptimizerNode> inputNodes) {
		return new int[0];
	}
	
	@Override
	public int[] getWriteSet(int input) {
		return null;
	}

	@Override
	public int[] getReadSet(int input) {
		return null;
	}

	@Override
	public int[] getWriteSet(int input, List<OptimizerNode> inputNodes) {
		return null;
	}
	
}
