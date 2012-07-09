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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * @author ringwald
 *
 */
public class UnionNode extends OptimizerNode {

	protected List<PactConnection> inConns;
	protected List<List<UnclosedBranchDescriptor>> openBranchesOfChildren = new ArrayList<List<UnclosedBranchDescriptor>>();
	
	public UnionNode(Contract descendant, List<Contract> children, Map<Contract, OptimizerNode> contractToNode) {
		super(descendant);
		this.inConns = new LinkedList<PactConnection>();
		
		for (Contract child : children) {
			OptimizerNode pred = contractToNode.get(child);
			// create the connection and add it
			PactConnection conn = new PactConnection(pred, this);
			this.inConns.add(conn);
			pred.addOutConn(conn);
			conn.setShipStrategy(ShipStrategy.FORWARD);
		}
		
		setLocalStrategy(LocalStrategy.NONE);
	}
	
	public UnionNode(UnionNode template, Stack<OptimizerNode> preds, GlobalProperties gp, LocalProperties lp) {
		super(template, gp, lp);
		this.inConns = new LinkedList<PactConnection>();
		for (int i = 0; i < preds.size(); i++){
			OptimizerNode pred = preds.get(i);
			PactConnection inConn = new PactConnection(template.inConns.get(i), pred, this);
			inConns.add(inConn);
		}
		
		setLocalStrategy(LocalStrategy.NONE);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Union";
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		return inConns;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		
		for (int i = 0; i < inConns.size(); i++) {
			List<InterestingProperties> inputProps = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
				this, i);
			inConns.get(i).addAllInterestingProperties(inputProps);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		List<UnclosedBranchDescriptor> result = new ArrayList<UnclosedBranchDescriptor>();
		
		for (PactConnection inConn : inConns) {
			List<UnclosedBranchDescriptor> openBranchForInput = inConn.getSourcePact().getBranchesForParent(this);
			openBranchesOfChildren.add(openBranchForInput);
			result = mergeLists(result, openBranchForInput);
		}
		
		this.openBranches = result;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public List<? extends OptimizerNode> getAlternativePlans(
			CostEstimator estimator) {
		
		List<OptimizerNode> alternatives = new LinkedList<OptimizerNode>();
		Stack<OptimizerNode> newInputs = new Stack<OptimizerNode>();
		Map<OptimizerNode, OptimizerNode> branchPlan = new HashMap<OptimizerNode, OptimizerNode>();
		List<List<? extends OptimizerNode>> inputs = new LinkedList<List<? extends OptimizerNode>>();
		for (PactConnection inConn : inConns) {
			inputs.add(inConn.getSourcePact().getAlternativePlans(estimator));
		}
		
		calcAlternatives(alternatives, newInputs, 0, inputs, null, branchPlan);
		
		// prune the plans
		prunePlanAlternatives(alternatives);

		return alternatives;
	}
	
	public void calcAlternatives(List<OptimizerNode> target, Stack<OptimizerNode> newInputs, int index, List<List<? extends OptimizerNode>> inputs, FieldList partitionedFieldsInCommon, Map<OptimizerNode, OptimizerNode> branchPlan) {
		List<? extends OptimizerNode> alternativesAtLevel = inputs.get(index);
		
		for (OptimizerNode alternative : alternativesAtLevel) {
		
			Map<OptimizerNode, OptimizerNode> newBranchPlan = new HashMap<OptimizerNode, OptimizerNode>(branchPlan);
			
			boolean isCompatible = true;
			if (openBranchesOfChildren.get(index) != null) {
				for (UnclosedBranchDescriptor branch : openBranchesOfChildren.get(index)) {
					OptimizerNode brancher = branch.getBranchingNode();
					if (newBranchPlan.containsKey(brancher)) {
						if (newBranchPlan.get(brancher) != alternative.branchPlan.get(brancher)) {
							isCompatible = false;
							break;
						}
					} 
					else {
						newBranchPlan.put(brancher, alternative.branchPlan.get(brancher));
					}
				}
			}
			
			
			if (isCompatible) {
				
				newInputs.push(alternative);
				
				FieldList newPartitionedFieldsInCommon = partitionedFieldsInCommon;
				
				// only property which would survive is a hash partitioning on every input
				GlobalProperties gpForInput = alternative.getGlobalProperties();
				if (index == 0 && gpForInput.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
					newPartitionedFieldsInCommon = gpForInput.getPartitionedFields();
				}
				else if (gpForInput.getPartitioning() != PartitionProperty.HASH_PARTITIONED
						|| gpForInput.getPartitionedFields().equals(partitionedFieldsInCommon) == false) {
					newPartitionedFieldsInCommon = null;
				}
	
				
				if (index < inputs.size() - 1) {
					calcAlternatives(target, newInputs, index + 1, inputs, newPartitionedFieldsInCommon, newBranchPlan);
				}
				else {
					GlobalProperties gp = new GlobalProperties();
					
					if (newPartitionedFieldsInCommon != null) {
						gp.setPartitioning(PartitionProperty.HASH_PARTITIONED, newPartitionedFieldsInCommon);
					}
					UnionNode unionNode = new UnionNode(this, newInputs, gp, new LocalProperties());
					unionNode.branchPlan = newBranchPlan;
					target.add(unionNode);
				}
				
				newInputs.pop();
			}
		}
		
	}
	
	@Override
	public PactType getPactType() {
		return PactType.Union;
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		
		if (visitor.preVisit(this)) {
			for (PactConnection inConn : this.inConns) {
				inConn.getSourcePact().accept(visitor);
			}
			visitor.postVisit(this);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getMemoryConsumerCount()
	 */
	@Override
	public int getMemoryConsumerCount() {
		return 0;
	}
	
	@Override
	protected void readStubAnnotations() {
		//DO NOTHING
		//do not read annotations for union nodes, as this node is artificially generated
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readConstantAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		//DO NOTHING
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldKept(int, int)
	 */
	@Override
	public boolean isFieldKept(int input, int fieldNumber) {
		return true;
	}
	
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		
		
		this.estimatedNumRecords = 0;
		this.estimatedOutputSize = 0;
		
		// init estimated cardinalities with the fields of the first input
		// remove the field which are unknown for other inputs later on
		for (FieldSet fieldSet : inConns.get(0).getSourcePact().getEstimatedCardinalities().keySet()) {
			this.estimatedCardinality.put(fieldSet, 0L);
		}
		
		
		for (PactConnection inConn : inConns) {
			
			OptimizerNode inputPact = inConn.getSourcePact();
			
			// sum up estimatedNumRecords for inputs
			long estimatedNumRecordForInput = inputPact.estimatedNumRecords;
			
			if (estimatedNumRecordForInput != -1 && this.estimatedNumRecords != -1) {
				this.estimatedNumRecords += estimatedNumRecordForInput;
			}
			else {
				this.estimatedNumRecords = -1;
			}
			
			// sum up estimatedOutputSize for inputs
			long estimatedOutputSizeForInput = inputPact.estimatedOutputSize;
			
			if (estimatedOutputSizeForInput != -1 && this.estimatedOutputSize != -1) {
				this.estimatedOutputSize += estimatedOutputSizeForInput;
			}
			else {
				this.estimatedOutputSize = -1;
			}
			
			
			//sum up cardinalities or remove them if they are unknown
			Set<FieldSet> toRemove = new HashSet<FieldSet>();
			
			for (Entry<FieldSet, Long> cardinality : this.estimatedCardinality.entrySet()) {
				long inputCard = inputPact.getEstimatedCardinality(cardinality.getKey());
				if (inputCard == -1) {
					toRemove.add(cardinality.getKey());
				}
				else {
					//to be conservative we assume the inputs are disjoint
					inputCard += cardinality.getValue();
					cardinality.setValue(inputCard);
				}
			}
			
			this.estimatedCardinality.keySet().removeAll(toRemove);
			
		}
		
		
	}
	
	
	@Override
	protected long computeNumberOfStubCalls() {
		return this.estimatedNumRecords;
	}
	
	
	@Override
	protected double computeAverageRecordWidth() {
		if (this.estimatedNumRecords == -1 || this.estimatedOutputSize == -1) return -1;
		
		final double width = this.estimatedOutputSize / (double) this.estimatedNumRecords;

		// a record must have at least one byte...
		if(width < 1)
			return 1;
		else 
			return width;  
		
	}
	
	@Override
	public void computeUniqueFields() {
		//DO NOTHING
		// we cannot guarantee uniqueness of fields for union
	}


}
