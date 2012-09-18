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
import eu.stratosphere.pact.compiler.PartitioningProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ForwardSS;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Union</i>. A Union is automatically
 * inserted before any node which has more than one incoming connection per
 * input.
 * 
 * @author Matthias Ringwald (matthias.ringwald@campus.tu-berlin.de)
 */
public class UnionNode extends OptimizerNode {

	protected List<PactConnection> inConns;	//all the incoming connections
	protected List<List<UnclosedBranchDescriptor>> openBranchesOfChildren //the open branches of the predecessors
								= new ArrayList<List<UnclosedBranchDescriptor>>();
	
	/**
	 * Creates a union node for the children of a given OptimizerNode 
	 * 
	 * @param descendant The descendant which needs the union for an input
	 * @param children The predecessors to be united
	 * @param contractToNode Mapping from contracts to their corresponding OptimizerNodes
	 */
	public UnionNode(Contract descendant, List<Contract> children, Map<Contract, OptimizerNode> contractToNode) {
		super(descendant);
		this.inConns = new LinkedList<PactConnection>();
		
		// create for each child of the descendant a connection to the union node
		for (Contract child : children) {
			OptimizerNode pred = contractToNode.get(child);
			
			PactConnection conn = new PactConnection(pred, this);
			this.inConns.add(conn);
			pred.addOutConn(conn);
			conn.setShipStrategy(new ForwardSS());
		}
		
		setLocalStrategy(LocalStrategy.NONE);
	}
	
	/**
	 * Copy constructor to create a copy of a UnionNode with different predecessors. The predecessors
	 * are assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 * 			The UnionNode to create a copy of.
	 * @param preds
	 *        The new predecessors.
	 * @param gp
	 *        The global properties of this copy.
	 * @param lp
	 *        The local properties of this copy.
	 */
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

		//no new interesting properties are generated, just push them to the predecessors
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
		
		//merge all open branches of the predecessors and store each individually 
		for (PactConnection inConn : inConns) {
			addClosedBranches(inConn.getSourcePact().closedBranchingNodes);
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
		
		//get the alternatives for the inputs
		for (PactConnection inConn : inConns) {
			inputs.add(inConn.getSourcePact().getAlternativePlans(estimator));
		}
		
		//recursively enumerate all possible plan combinations
		calcAlternatives(alternatives, newInputs, 0, inputs, null, branchPlan);
		
		// prune the plans
		prunePlanAlternatives(alternatives);

		return alternatives;
	}
	
	/**
	 * Recursive function for enumerating all possible plan combinations
	 * 
	 * @param target The list of generated combinations
	 * @param subplanStack A stack of the chosen sub plan alternative
	 * @param index The index of the input to be chosen
	 * @param alternativesForInputs The alternatives for the inputs
	 * @param partitionedFieldsInCommon The fields on which all inputs are hash partitioned
	 * @param branchPlan The branch plan for the new node
	 */
	public void calcAlternatives(List<OptimizerNode> target, Stack<OptimizerNode> subplanStack, int index,
			List<List<? extends OptimizerNode>> alternativesForInputs, FieldList partitionedFieldsInCommon, Map<OptimizerNode, OptimizerNode> branchPlan) {
	
		//enumerate all alternative for the current input
		List<? extends OptimizerNode> alternativesAtLevel = alternativesForInputs.get(index);
		for (OptimizerNode alternative : alternativesAtLevel) {
		
			Map<OptimizerNode, OptimizerNode> newBranchPlan = new HashMap<OptimizerNode, OptimizerNode>(branchPlan);
		
			//check if the current alternative is compatible with the other inputs 
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
				// choose alternative
				subplanStack.push(alternative);
				
				FieldList newPartitionedFieldsInCommon = partitionedFieldsInCommon;
				
				// only property which would survive is a hash partitioning on every input
				GlobalProperties gpForInput = alternative.getGlobalPropertiesForParent(this);
				if (index == 0 && gpForInput.getPartitioning() == PartitioningProperty.HASH_PARTITIONED) {
					newPartitionedFieldsInCommon = gpForInput.getPartitionedFields();
				}
				else if (gpForInput.getPartitioning() != PartitioningProperty.HASH_PARTITIONED
						|| gpForInput.getPartitionedFields().equals(partitionedFieldsInCommon) == false) {
					newPartitionedFieldsInCommon = null;
				}
	
				
				if (index < alternativesForInputs.size() - 1) {
					//recursive descent
					calcAlternatives(target, subplanStack, index + 1, alternativesForInputs, newPartitionedFieldsInCommon, newBranchPlan);
				}
				else {
					//we have found a valid combination, create the according UnionNode for it
					GlobalProperties gp = new GlobalProperties();
					
					if (newPartitionedFieldsInCommon != null) {
						gp.setPartitioning(PartitioningProperty.HASH_PARTITIONED, newPartitionedFieldsInCommon);
					}
					UnionNode unionNode = new UnionNode(this, subplanStack, gp, new LocalProperties());
					unionNode.branchPlan = newBranchPlan;
					target.add(unionNode);
				}
				
				//undo last selection of the alternatives
				subplanStack.pop();
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
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		
		
		this.estimatedNumRecords = 0;
		this.estimatedOutputSize = 0;
		
		// init estimated cardinalities with the fields of the first input
		// remove the field which are unknown for other inputs later on
		for (FieldSet fieldSet : inConns.get(0).getSourcePact().getEstimatedCardinalities().keySet()) {
			this.estimatedCardinality.put(fieldSet, -1L);
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
					//to be conservative for joins we use the max for new column cardinality
					inputCard = Math.max(inputCard, cardinality.getValue());
					cardinality.setValue(inputCard);
				}
			}
			
			this.estimatedCardinality.keySet().removeAll(toRemove);
			
		}
		
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeNumberOfStubCalls()
	 */
	@Override
	protected long computeNumberOfStubCalls() {
		return this.estimatedNumRecords;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeNumberOfStubCalls()
	 */
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
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUniqueFields()
	 */
	@Override
	public void computeUniqueFields() {
		//DO NOTHING
		// we cannot guarantee uniqueness of fields for union
	}


}
