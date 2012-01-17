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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSet.ConstantSetMode;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.util.FieldSetOperations;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a two different inputs, such as MATCH or CROSS.
 * The two inputs are not substitutable in their sides.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class TwoInputNode extends OptimizerNode
{
	private List<OptimizerNode> cachedPlans; // a cache for the computed alternative plans

	final protected List<PactConnection> input1 = new ArrayList<PactConnection>(); // The first input edge

	final protected List<PactConnection> input2 = new ArrayList<PactConnection>(); // The second input edge

	private List<List<PactConnection>> inputs; // the cached list of inputs
	
	protected int[] keySet1; // The set of key fields for the first input (order is relevant!)
	
	protected int[] keySet2; // The set of key fields for the second input (order is relevant!)
	
	// ------------- Stub Annotations
	
	protected int[] readSet1; // set of fields of the first input that are read by the stub
	
	protected int[] updateSet1; // set of fields of the first input that are modified by the stub
	
	protected int[] constantSet1; // set of fields of the first input that remain constant from input to output
	
	protected ConstantSetMode constantSet1Mode;
	
	protected int[] readSet2; // set of fields of the first input that are read by the stub
	
	protected int[] updateSet2; // set of fields of the first input that are modified by the stub
	
	protected int[] constantSet2; // set of fields of the first input that remain constant from input to output

	protected ConstantSetMode constantSet2Mode;
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public TwoInputNode(DualInputContract<?> pactContract) {
		super(pactContract);

		this.inputs = new ArrayList<List<PactConnection>>(2);
		
		this.keySet1 = pactContract.getKeyColumnNumbers(0);
		this.keySet2 = pactContract.getKeyColumnNumbers(1);
		
		readReadSetAnnotation();
		readConstantSetAnnotation();
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
	protected TwoInputNode(TwoInputNode template, List<OptimizerNode> pred1, List<OptimizerNode> pred2, List<PactConnection> conn1,
			List<PactConnection> conn2, GlobalProperties globalProps, LocalProperties localProps)
	{
		super(template, globalProps, localProps);
		
		this.readSet1 = template.readSet1;
		this.readSet2 = template.readSet2;
		this.updateSet1 = template.updateSet1;
		this.updateSet2 = template.updateSet2;
		this.constantSet1 = template.constantSet1;
		this.constantSet2 = template.constantSet2;
		this.constantSet1Mode = template.constantSet1Mode;
		this.constantSet2Mode = template.constantSet2Mode;
		this.keySet1 = template.keySet1;
		this.keySet2 = template.keySet2;

		this.inputs = new ArrayList<List<PactConnection>>(2);
		int i = 0;
		
		if(pred1 != null) {
			for(PactConnection c : conn1) {
				PactConnection cc = new PactConnection(c, pred1.get(i++), this); 
				this.input1.add(cc);
			}
			this.inputs.add(this.input1);
		}
		
		if(pred2 != null) {
			i = 0;
			for(PactConnection c : conn2) {
				PactConnection cc = new PactConnection(c, pred2.get(i++), this); 
				this.input2.add(cc);
			}
			this.inputs.add(this.input2);
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
					Iterator<OptimizerNode> it1 = pred1.iterator();
					// we take the candidate from pred1. if both have it, we could take it from either,
					// as they have to be the same
					while(it1.hasNext()) {
						OptimizerNode n = it1.next();
						
						if(n.branchPlan != null) {
							// predecessor 1 has branching children, see if it got the branch we are looking for
							selectedCandidate = n.branchPlan.get(brancher);
							this.branchPlan.put(brancher, selectedCandidate);
						}
					}
				}
				
				if(selectedCandidate == null && pred2 != null) {
					Iterator<OptimizerNode> it2 = pred2.iterator();
		
					while(it2.hasNext()) {
						OptimizerNode n = it2.next();
						
						if(n.branchPlan != null) {
							// predecessor 2 has branching children, see if it got the branch we are looking for
							selectedCandidate = n.branchPlan.get(brancher);
							this.branchPlan.put(brancher, selectedCandidate);
						}
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
	public List<PactConnection> getFirstInputConnection() {
		return this.input1;
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @return The second input connection.
	 */
	public List<PactConnection> getSecondInputConnection() {
		return this.input2;
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @param conn
	 *        The first input connection.
	 */
	public void setFirstInputConnection(PactConnection conn) {
		this.input1.add(conn);
	}

	/**
	 * Sets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @param conn
	 *        The second input connection.
	 */
	public void setSecondInputConnection(PactConnection conn) {
		this.input2.add(conn);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<List<PactConnection>> getIncomingConnections() {
		return this.inputs;
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
		
		for(Contract cl : leftPreds) {
			OptimizerNode pred1 = contractToNode.get(cl);
			// create the connections and add them
			PactConnection conn1 = new PactConnection(pred1, this);
			this.input1.add(conn1);
			pred1.addOutgoingConnection(conn1);
		}

		for(Contract cr : rightPreds) {
			OptimizerNode pred2 = contractToNode.get(cr);
			// create the connections and add them
			PactConnection conn2 = new PactConnection(pred2, this);
			this.input2.add(conn2);
			pred2.addOutgoingConnection(conn2);
		}

		// see if there is a hint that dictates which shipping strategy to use for BOTH inputs
		Configuration conf = getPactContract().getParameters();
		String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
			} else {
				throw new CompilerException("Unknown hint for shipping strategy: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the FIRST input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				for(PactConnection c : this.input1) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input one: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the SECOND input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.FORWARD);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.BROADCAST);
				}
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
				for(PactConnection c : this.input2) {
					c.setShipStrategy(ShipStrategy.PARTITION_HASH);
				}
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
		final int inputSize1 = this.input1.size();
		@SuppressWarnings("unchecked")
		List<? extends OptimizerNode>[] inPlans1 = new List[inputSize1];
		for(int i = 0; i < inputSize1; ++i) {
			inPlans1[i] = this.input1.get(i).getSourcePact().getAlternativePlans(estimator);
		}

		// build all possible alternative plans for first input of this node
		List<List<OptimizerNode>> alternativeSubPlanCominations1 = new ArrayList<List<OptimizerNode>>();
		getAlternativeSubPlanCombinationsRecursively(inPlans1, new ArrayList<OptimizerNode>(0), alternativeSubPlanCominations1);
		
		
		// step down to all producer nodes for first input and calculate alternative plans
		final int inputSize2 = this.input2.size();
		@SuppressWarnings("unchecked")
		List<? extends OptimizerNode>[] inPlans2 = new List[inputSize2];
		for(int i = 0; i < inputSize2; ++i) {
			inPlans2[i] = this.input2.get(i).getSourcePact().getAlternativePlans(estimator);
		}

		// build all possible alternative plans for first input of this node
		List<List<OptimizerNode>> alternativeSubPlanCominations2 = new ArrayList<List<OptimizerNode>>();
		getAlternativeSubPlanCombinationsRecursively(inPlans2, new ArrayList<OptimizerNode>(0), alternativeSubPlanCominations2);

		
		
		List<OptimizerNode> outputPlans = new ArrayList<OptimizerNode>();

		computeValidPlanAlternatives(alternativeSubPlanCominations1, alternativeSubPlanCominations2, estimator,  outputPlans);
		
		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (this.getOutgoingConnections() != null && this.getOutgoingConnections().size() > 1) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}
	
	/**
	 * Takes a list with all sub-plan-combinations (each is a list by itself) and produces alternative
	 * plans for the current node using the single sub-plans-combinations.
	 *  
	 * @param alternativeSubPlanCominations1	 	List with all sub-plan-combinations for first input
	 * @param alternativeSubPlanCominations2	 	List with all sub-plan-combinations for secodn input
	 * @param estimator								Cost estimator to be used
	 * @param outputPlans							The generated output plans (is expected to be a list where new plans can be added)
	 */
	protected abstract void computeValidPlanAlternatives(List<List<OptimizerNode>> alternativeSubPlanCominations1,
			List<List<OptimizerNode>> alternativeSubPlanCominations2, CostEstimator estimator, List<OptimizerNode> outputPlans);
	
	/**
	 * Checks if all {@code PactConnections} have compatible ShipStrategies to each other.
	 * 
	 * @param input		The list of PactConnections to test.
	 * 
	 * @return		The ShipStrategy to be used for all inputs<br />
	 * 				{@code null} if the given ShipStrategies are incompatible
	 */
	protected ShipStrategy checkShipStrategyCompatibility(List<PactConnection> input) {
		ShipStrategy ss = ShipStrategy.NONE;
		final int size = input.size();
		
		// look if an input strategy is set
		int i;
		for(i = 0; i < size; ++i) {
			ss = input.get(i).getShipStrategy();
			
			if(ss != ShipStrategy.NONE) { // we found one...
				++i;
				break;
			}
			
		}
		
		if(ss != ShipStrategy.NONE) {
			// ss1 is set; lets check if all remaining inputs are compatible
			for( /* i is already at the right value */ ; i < size; ++i) {
				ShipStrategy s = input.get(i).getShipStrategy();
				
				if(s != ShipStrategy.NONE && s != ss) // if ship strategies is set and not equal we hit an incompatibility
					return null;
			}
		}

		return ss;
	}
	/**
	 * Checks if all predecessor nodes have a valid outputSize estimation value set.
	 * 
	 * @param allPreds		the first list of all predecessor to check
	 * 
	 * @return	{@code true} if all values are valid, {@code false} otherwise
	 */
	protected boolean haveValidOutputEstimates(List<OptimizerNode> allPreds) {
	
		for(OptimizerNode n : allPreds) {
			if(n.getEstimatedOutputSize() == -1) {
				return false;
			}
		}
		
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


		List<UnclosedBranchDescriptor> result1 = new ArrayList<UnclosedBranchDescriptor>();
		for(PactConnection c : this.input1) {
			result1 = mergeLists(result1, c.getSourcePact().getBranchesForParent(this));
		}
		List<UnclosedBranchDescriptor> result2 = new ArrayList<UnclosedBranchDescriptor>();
		for(PactConnection c : this.input2) {
			result2 = mergeLists(result2, c.getSourcePact().getBranchesForParent(this));
		}

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
			if (this.input1 != null) {
				for(PactConnection c : this.input1) {
					if(c.getSourcePact() != null) {
						c.getSourcePact().accept(visitor);
					}
				}
			}
			if (this.input2 != null) {
				for(PactConnection c : this.input2) {
					if(c.getSourcePact() != null) {
						c.getSourcePact().accept(visitor);
					}
				}
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

		// we have to look for closing branches for all input-pair-combinations,
		// including input-pairs of the same input which are unioned

		final int sizeInput1 = this.input1.size();
		final int sizeInput2 = this.input2.size();
		
		// all unioned inputs from input1
		for(int i = 0; i < sizeInput1; ++i) {
			PactConnection pc1 = this.input1.get(i);
			for(int j = i+1; j < sizeInput1; ++j) {
				PactConnection pc2 = this.input1.get(j);

				// get the children and check their existence
				OptimizerNode child1 = pc1.getSourcePact();
				OptimizerNode child2 = pc2.getSourcePact();
				
				if (child1 == null || child2 == null) {
					continue;
				}
				
				// get the cumulative costs of the last joined branching node
				OptimizerNode lastCommonChild = child1.branchPlan.get(this.lastJoinedBranchNode);
				Costs douleCounted = lastCommonChild.getCumulativeCosts();
				getCumulativeCosts().subtractCosts(douleCounted);
			}
		}
		
		// all unioned inputs from input2
		for(int i = 0; i < sizeInput2; ++i) {
			PactConnection pc1 = this.input2.get(i);
			for(int j = i+1; j < sizeInput2; ++j) {
				PactConnection pc2 = this.input2.get(j);

				// get the children and check their existence
				OptimizerNode child1 = pc1.getSourcePact();
				OptimizerNode child2 = pc2.getSourcePact();
				
				if (child1 == null || child2 == null) {
					continue;
				}
				
				// get the cumulative costs of the last joined branching node
				OptimizerNode lastCommonChild = child1.branchPlan.get(this.lastJoinedBranchNode);
				Costs douleCounted = lastCommonChild.getCumulativeCosts();
				getCumulativeCosts().subtractCosts(douleCounted);
			}
		}

		// all input pairs from input1 and input2
		for(PactConnection pc1 : this.input1) {
			for(PactConnection pc2 : this.input2) {

				// get the children and check their existence
				OptimizerNode child1 = pc1.getSourcePact();
				OptimizerNode child2 = pc2.getSourcePact();
				
				if (child1 == null || child2 == null) {
					continue;
				}
				
				// get the cumulative costs of the last joined branching node
				OptimizerNode lastCommonChild = child1.branchPlan.get(this.lastJoinedBranchNode);
				Costs douleCounted = lastCommonChild.getCumulativeCosts();
				getCumulativeCosts().subtractCosts(douleCounted);
			}
		}
	}
	
	private void readReadSetAnnotation() {
		
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get readSet annotation from stub
		ReadSetFirst readSet1Annotation = c.getUserCodeClass().getAnnotation(ReadSetFirst.class);
		ReadSetSecond readSet2Annotation = c.getUserCodeClass().getAnnotation(ReadSetSecond.class);
		
		// extract readSets from annotations
		if(readSet1Annotation == null) {
			this.readSet1 = null;
		} else {
			this.readSet1 = readSet1Annotation.fields();
			Arrays.sort(this.readSet1);
		}
		
		if(readSet2Annotation == null) {
			this.readSet2 = null;
		} else {
			this.readSet2 = readSet2Annotation.fields();
			Arrays.sort(this.readSet2);
		}

		if(c instanceof MatchContract || c instanceof CoGroupContract) {
			// merge read and key sets
			if(this.readSet1 != null) {
				int[] keySet1 = c.getKeyColumnNumbers(0);
				Arrays.sort(keySet1);
				this.readSet1 = FieldSetOperations.unionSets(keySet1, this.readSet1);
			}
			
			if(this.readSet2 != null) {
				int[] keySet2 = c.getKeyColumnNumbers(1);
				Arrays.sort(keySet2);
				this.readSet2 = FieldSetOperations.unionSets(keySet2, this.readSet1);
			}
		} 
	}
	
	private void readConstantSetAnnotation() {
		
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get updateSet annotation from stub
		ConstantSetFirst updateSet1Annotation = c.getUserCodeClass().getAnnotation(ConstantSetFirst.class);
		ConstantSetSecond updateSet2Annotation = c.getUserCodeClass().getAnnotation(ConstantSetSecond.class);
		
		if(updateSet1Annotation == null) {
			this.updateSet1 = null;
			this.constantSet1 = null;
		} else {
			
			switch(updateSet1Annotation.setMode()) {
			case Update:
				// we have a write set
				this.updateSet1 = updateSet1Annotation.fields();
				this.constantSet1 = null;
				Arrays.sort(this.updateSet1);
				this.constantSet1Mode = ConstantSetMode.Update;
				break;
			case Constant:
				// we have a constant set
				this.updateSet1 = null;
				this.constantSet1 = updateSet1Annotation.fields();
				Arrays.sort(this.constantSet1);
				this.constantSet1Mode = ConstantSetMode.Constant;
				break;
			default:
				this.updateSet1 = null;
				this.constantSet1 = null;
				this.constantSet1Mode = null;
				break;
			}
			
		}
		
		if(updateSet2Annotation == null) {
			this.updateSet2 = null;
			this.constantSet2 = null;
		} else {
			
			switch(updateSet2Annotation.setMode()) {
			case Update:
				// we have a write set
				this.updateSet2 = updateSet2Annotation.fields();
				this.constantSet2 = null;
				Arrays.sort(this.updateSet2);
				this.constantSet2Mode = ConstantSetMode.Update;
				break;
			case Constant:
				// we have a constant set
				this.updateSet2 = null;
				this.constantSet2 = updateSet2Annotation.fields();
				Arrays.sort(this.constantSet2);
				this.constantSet2Mode = ConstantSetMode.Constant;
				break;
			default:
				this.updateSet2 = null;
				this.constantSet2 = null;
				this.constantSet2Mode = null;
				break;
			}
		}
	}
	
	@Override
	public void deriveOutputSchema() {
		if(this.addSet == null) {
			this.outputSchema = null;
			return;
		} else {
			outputSchema = this.addSet;
		}
		
		for(PactConnection pc : this.getFirstInputConnection()) {
			if(pc.getSourcePact().outputSchema == null) {
				this.outputSchema = null;
				return;
			}
			outputSchema = FieldSetOperations.unionSets(outputSchema, pc.getSourcePact().outputSchema);
		}
		for(PactConnection pc : this.getSecondInputConnection()) {
			if(pc.getSourcePact().outputSchema == null) {
				this.outputSchema = null;
				return;
			}
			outputSchema = FieldSetOperations.unionSets(outputSchema, pc.getSourcePact().outputSchema);
		}
	}
	
	public int[] getInputReadSet(int input) {
		switch(input) {
		case 0: return readSet1;
		case 1: return readSet2;
		default: throw new IndexOutOfBoundsException();
		}
	}
	
	public int[] getInputUpdateSet(int input) {
		switch(input) {
		case 0: 
			
			if(this.constantSet1Mode == null)
				return null;

			switch(this.constantSet1Mode) {
			case Constant:
				int[] inputSchema = this.input1.get(0).getSourcePact().outputSchema;
				if(inputSchema == null) {
					return null;
				} else {
					return FieldSetOperations.setDifference(inputSchema, this.constantSet1);
				}
			case Update:
				return this.updateSet1;
			}
			
			return null;
		case 1: 
			
			if(this.constantSet2Mode == null)
				return null;
	
			switch(this.constantSet2Mode) {
			case Constant:
				int[] inputSchema = this.input2.get(0).getSourcePact().outputSchema;
				if(inputSchema == null) {
					return null;
				} else {
					return FieldSetOperations.setDifference(inputSchema, this.constantSet2);
				}
			case Update:
				return this.updateSet2;
			}
			
			return null;
		
		default: throw new IndexOutOfBoundsException();
		}
	}
	
	public int[] getInputConstantSet(int input) {
		switch(input) {
		case 0: 
			if(this.constantSet1Mode == null)
				return null;

			switch(this.constantSet1Mode) {
			case Update:
				int[] inputSchema = this.input1.get(0).getSourcePact().outputSchema;
				if(inputSchema == null) {
					return null;
				} else {
					return FieldSetOperations.setDifference(inputSchema, this.updateSet1);
				}
			case Constant:
				return this.constantSet1;
			}
			
			return null;
			
		case 1: 
			if(this.constantSet2Mode == null)
				return null;

			switch(this.constantSet2Mode) {
			case Update:
				int[] inputSchema = this.input2.get(0).getSourcePact().outputSchema;
				if(inputSchema == null) {
					return null;
				} else {
					return FieldSetOperations.setDifference(inputSchema, this.updateSet2);
				}
			case Constant:
				return this.constantSet2;
			}
			
			return null;
		default: throw new IndexOutOfBoundsException();
		}
	}
	
	public int[] getInputKeySet(int input) {
		switch(input) {
		case 0: return keySet1;
		case 1: return keySet2;
		default: throw new IndexOutOfBoundsException();
		}
	}
	
}
