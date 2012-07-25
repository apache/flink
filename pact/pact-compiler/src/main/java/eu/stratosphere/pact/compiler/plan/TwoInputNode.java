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
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a two different inputs, such as MATCH or CROSS.
 * The two inputs are not substitutable in their sides.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class TwoInputNode extends OptimizerNode
{
	private List<OptimizerNode> cachedPlans; // a cache for the computed alternative plans

	protected PactConnection input1 = null; // The first input edge

	protected PactConnection input2 = null; // The second input edge

	protected FieldList keySet1; // The set of key fields for the first input (order is relevant!)
	
	protected FieldList keySet2; // The set of key fields for the second input (order is relevant!)
	
	// ------------- Stub Annotations
	
	protected FieldSet constant1; // set of fields that are left unchanged by the stub
	
	protected FieldSet constant2; // set of fields that are left unchanged by the stub
	
	protected FieldSet notConstant1; // set of fields that are changed by the stub
	
	protected FieldSet notConstant2; // set of fields that are changed by the stub
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public TwoInputNode(DualInputContract<?> pactContract) {
		super(pactContract);

		this.keySet1 = new FieldList(pactContract.getKeyColumnNumbers(0));
		this.keySet2 = new FieldList(pactContract.getKeyColumnNumbers(1));
		
	}

	/**
	 * Copy constructor to create a copy of a node with different predecessors. The predecessors
	 * is assumed to be of the same type as in the template node and merely copies with different
	 * strategies, as they are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred1
	 *        The new predecessor for the first input.
	 * @param pred2
	 *        The new predecessor for the second input.
	 * @param conn1
	 *        The old connection of the first input to copy properties from.
	 * @param conn2
	 *        The old connection of the second input to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected TwoInputNode(TwoInputNode template, OptimizerNode pred1, OptimizerNode pred2, PactConnection conn1,
			PactConnection conn2, GlobalProperties globalProps, LocalProperties localProps)
	{
		super(template, globalProps, localProps);
		
		this.constant1 = template.constant1;
		this.constant2 = template.constant2;
		this.keySet1 = template.keySet1;
		this.keySet2 = template.keySet2;

		if(pred1 != null) {
			this.input1 = new PactConnection(conn1, pred1, this);
		}
		
		if(pred2 != null) {
			this.input2 = new PactConnection(conn2, pred2, this);
		}

		// merge the branchPlan maps according the the template's uncloseBranchesStack
		if (template.openBranches != null)
		{
			if (this.branchPlan == null) {
				this.branchPlan = new HashMap<OptimizerNode, OptimizerNode>(8);
			}

			for (UnclosedBranchDescriptor uc : template.openBranches) {
				OptimizerNode brancher = uc.branchingNode;
				OptimizerNode selectedCandidate = null;

				if(pred1 != null) {
					if(pred1.branchPlan != null) {
						// predecessor 1 has branching children, see if it got the branch we are looking for
						selectedCandidate = pred1.branchPlan.get(brancher);
						this.branchPlan.put(brancher, selectedCandidate);
					}
				}
				
				if(selectedCandidate == null && pred2 != null) {
					if(pred2.branchPlan != null) {
						// predecessor 2 has branching children, see if it got the branch we are looking for
						selectedCandidate = pred2.branchPlan.get(brancher);
						this.branchPlan.put(brancher, selectedCandidate);
					}
				}

				if (selectedCandidate == null) {
					throw new CompilerException(
						"Candidates for a node with open branches are missing information about the selected candidate ");
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @return The first input connection.
	 */
	public PactConnection getFirstInConn() {
		return this.input1;
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @return The second input connection.
	 */
	public PactConnection getSecondInConn() {
		return this.input2;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @param conn
	 *        The first input connection.
	 */
	public void setFirstInConn(PactConnection conn) {
		this.input1 = conn;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @param conn
	 *        The second input connection.
	 */
	public void setSecondInConn(PactConnection conn) {
		this.input2 = conn;
	}
	
	/**
	 * TODO
	 */
	public OptimizerNode getFirstPredNode() {
		if(this.input1 != null)
			return this.input1.getSourcePact();
		else
			return null;
	}
	
	/**
	 * TODO
	 */
	public OptimizerNode getSecondPredNode() {
		if(this.input2 != null)
			return this.input2.getSourcePact();
		else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		ArrayList<PactConnection> inputs = new ArrayList<PactConnection>(2);
		inputs.add(input1);
		inputs.add(input2);
		return inputs;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		// get the predecessors
		DualInputContract<?> contr = (DualInputContract<?>) getPactContract();
		
		List<Contract> leftPreds = contr.getFirstInputs();
		List<Contract> rightPreds = contr.getSecondInputs();
		
		OptimizerNode pred1;
		if (leftPreds.size() == 1) {
			pred1 = contractToNode.get(leftPreds.get(0));
		} else {
			pred1 = new UnionNode(getPactContract(), leftPreds, contractToNode);
			pred1.setDegreeOfParallelism(this.getDegreeOfParallelism());
			//push id down to newly created union node
			pred1.SetId(this.id);
			pred1.setInstancesPerMachine(instancesPerMachine);
			this.id++;
		}
		// create the connection and add it
		PactConnection conn1 = new PactConnection(pred1, this);
		this.input1 = conn1;
		pred1.addOutConn(conn1);
		
		OptimizerNode pred2;
		if (rightPreds.size() == 1) {
			pred2 = contractToNode.get(rightPreds.get(0));
		} else {
			pred2 = new UnionNode(getPactContract(), rightPreds, contractToNode);
			pred2.setDegreeOfParallelism(this.getDegreeOfParallelism());
			//push id down to newly created union node
			pred2.SetId(this.id);
			pred2.setInstancesPerMachine(instancesPerMachine);
			this.id++;
		}
		// create the connection and add it
		PactConnection conn2 = new PactConnection(pred2, this);
		this.input2 = conn2;
		pred2.addOutConn(conn2);

		// see if there is a hint that dictates which shipping strategy to use for BOTH inputs
		Configuration conf = getPactContract().getParameters();
		String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.FORWARD);
				this.input2.setShipStrategy(ShipStrategy.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.BROADCAST);
				this.input2.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.PARTITION_HASH);
				this.input2.setShipStrategy(ShipStrategy.PARTITION_HASH);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the FIRST input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategy.PARTITION_HASH);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input one: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the SECOND input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategy.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategy.PARTITION_HASH);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input two: " + shipStrategy);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans()
	 */
	@Override
	final public List<OptimizerNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// step down to all producer nodes for first input and calculate alternative plans
		List<? extends OptimizerNode> subPlans1 = this.getFirstPredNode().getAlternativePlans(estimator);
		
		// step down to all producer nodes for second input and calculate alternative plans
		List<? extends OptimizerNode> subPlans2 = this.getSecondPredNode().getAlternativePlans(estimator);

		List<OptimizerNode> outputPlans = new ArrayList<OptimizerNode>();
		computeValidPlanAlternatives(subPlans1, subPlans2, estimator,  outputPlans);
		
		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (this.getOutConns() != null && this.getOutConns().size() > 1) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}
	
	/**
	 * Takes a list with all sub-plan-combinations (each is a list by itself) and produces alternative
	 * plans for the current node using the single sub-plans-combinations.
	 *  
	 * @param altSubPlans1	 	All subplans of the first input
	 * @param altSubPlans2	 	All subplans of the second input
	 * @param estimator			Cost estimator to be used
	 * @param outputPlans		The generated output plans
	 */
	protected abstract void computeValidPlanAlternatives(List<? extends OptimizerNode> altSubPlans1,
			List<? extends OptimizerNode> altSubPlans2, CostEstimator estimator, List<OptimizerNode> outputPlans);
		
	/**
	 * Checks if the subPlan has a valid outputSize estimation.
	 * 
	 * @param subPlan		the subPlan to check
	 * 
	 * @return	{@code true} if all values are valid, {@code false} otherwise
	 */
	protected boolean haveValidOutputEstimates(OptimizerNode subPlan) {
	
		if(subPlan.getEstimatedOutputSize() == -1)
			return false;
		else
			return true;
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

		addClosedBranches(this.getFirstPredNode().closedBranchingNodes);
		addClosedBranches(this.getSecondPredNode().closedBranchingNodes);
		
		List<UnclosedBranchDescriptor> result1 = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge is really necessary
		result1 = mergeLists(result1, this.getFirstPredNode().getBranchesForParent(this));
		
		List<UnclosedBranchDescriptor> result2 = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge is really necessary
		result2 = mergeLists(result2, this.getSecondPredNode().getBranchesForParent(this));

		this.openBranches = mergeLists(result1, result2);
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor
	 * )
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		boolean descend = visitor.preVisit(this);

		if (descend) {
			if (this.getFirstPredNode() != null) {
				this.getFirstPredNode().accept(visitor);
			}
			if (this.getSecondPredNode() != null) {
				this.getSecondPredNode().accept(visitor);
			}

			visitor.postVisit(this);
		}
	}

	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.OptimizerNode}.
	 * Since nodes with multiple inputs may join branched plans, care must be taken not to double-count the costs of the subtree rooted
	 * at the last unjoined branch.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setCosts(eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		
		// check, if this node has no branch beneath it, no double-counted cost then
		if (this.lastJoinedBranchNode == null) {
			return;
		}

		// TODO: Check this!
		// get the cumulative costs of the last joined branching node
		OptimizerNode lastCommonChild = this.getFirstPredNode().branchPlan.get(this.lastJoinedBranchNode);
		Costs douleCounted = lastCommonChild.getCumulativeCosts();
		getCumulativeCosts().subtractCosts(douleCounted);
		
	}
	
	// ---------------------- Stub Annotation Handling
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get readSet annotation from stub
		ConstantFieldsFirst constantSet1Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsFirst.class);
		ConstantFieldsSecond constantSet2Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsSecond.class);
		
		// extract readSets from annotations
		if(constantSet1Annotation == null) {
			this.constant1 = null;
		} else {
			this.constant1 = new FieldSet(constantSet1Annotation.fields());
		}
		
		if(constantSet2Annotation == null) {
			this.constant2 = null;
		} else {
			this.constant2 = new FieldSet(constantSet2Annotation.fields());
		}
		
		
		// get readSet annotation from stub
		ConstantFieldsFirstExcept notConstantSet1Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsFirstExcept.class);
		ConstantFieldsSecondExcept notConstantSet2Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsSecondExcept.class);
		
		// extract readSets from annotations
		if(notConstantSet1Annotation == null) {
			this.notConstant1 = null;
		} else {
			this.notConstant1 = new FieldSet(notConstantSet1Annotation.fields());
		}
		
		if(notConstantSet2Annotation == null) {
			this.notConstant2 = null;
		} else {
			this.notConstant2 = new FieldSet(notConstantSet2Annotation.fields());
		}
		
		
		if (this.notConstant1 != null && this.constant1 != null) {
			throw new CompilerException("Either ConstantFieldsFirst or ConstantFieldsFirstExcept can be specified, not both.");
		}
		
		if (this.notConstant2 != null && this.constant2 != null) {
			throw new CompilerException("Either ConstantFieldsSecond or ConstantFieldsSecondExcept can be specified, not both.");
		}
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		CompilerHints hints = getPactContract().getCompilerHints();

		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		}
	
		double avgRecordWidth = -1;
		
		if(this.getFirstPredNode() != null && 
				this.getFirstPredNode().estimatedOutputSize != -1 &&
				this.getFirstPredNode().estimatedNumRecords != -1) {
			avgRecordWidth = (this.getFirstPredNode().estimatedOutputSize / this.getFirstPredNode().estimatedNumRecords);
			
		} else {
			return -1;
		}
		
		if(this.getSecondPredNode() != null && 
				this.getSecondPredNode().estimatedOutputSize != -1 &&
				this.getSecondPredNode().estimatedNumRecords != -1) {
			
			avgRecordWidth += (this.getSecondPredNode().estimatedOutputSize / this.getSecondPredNode().estimatedNumRecords);
			
		} else {
			return -1;
		}

		return (avgRecordWidth < 1) ? 1 : avgRecordWidth;
	}

	/**
	 * Returns the key fields of the given input.
	 * 
	 * @param input The input for which key fields must be returned.
	 * @return the key fields of the given input.
	 */
	public FieldList getInputKeySet(int input) {
		switch(input) {
		case 0: return keySet1;
		case 1: return keySet2;
		default: throw new IndexOutOfBoundsException();
		}
	}
	
	public boolean isFieldKept(int input, int fieldNumber) {
		
		switch(input) {
		case 0:
			if (this.constant1 == null) {
				if (this.notConstant1 == null) {
					return false;
				}
				return this.notConstant1.contains(fieldNumber) == false;
			}
			
			return this.constant1.contains(fieldNumber);
		case 1:
			if (this.constant2 == null) {
				if (this.notConstant2 == null) {
					return false;
				}
				return this.notConstant2.contains(fieldNumber) == false;
			}
			
			return this.constant2.contains(fieldNumber);
		default:
			throw new IndexOutOfBoundsException();
		}
	}
	
}
