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
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitCopiesFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitCopiesSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitProjectionsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ExplicitProjectionsSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperationFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperationSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadsSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;
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

	protected int[] keySet1; // The set of key fields for the first input (order is relevant!)
	
	protected int[] keySet2; // The set of key fields for the second input (order is relevant!)
	
	// ------------- Stub Annotations
	
	protected int[] reads1; // set of fields that are read by the stub
	
	protected int[] explProjections1; // set of fields that are explicitly projected from the first input
	
	protected int[] explCopies1; // set of fields that are copied from the first input to output 
	
	protected ImplicitOperationMode implOpMode1; // implicit operation of the stub on the first input
	
	protected int[] reads2; // set of fields that are read by the stub
	
	protected int[] explProjections2; // set of fields that are explicitly projected from the second input
	
	protected int[] explCopies2; // set of fields that are copied from the second input to output 
	
	protected ImplicitOperationMode implOpMode2; // implicit operation of the stub on the second input
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public TwoInputNode(DualInputContract<?> pactContract) {
		super(pactContract);

		this.keySet1 = pactContract.getKeyColumnNumbers(0);
		this.keySet2 = pactContract.getKeyColumnNumbers(1);
		
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
		
		this.reads1 = template.reads1;
		this.reads2 = template.reads2;
		this.explCopies1 = template.explCopies1;
		this.explCopies2 = template.explCopies2;
		this.explProjections1 = template.explProjections1;
		this.explProjections2 = template.explProjections2;
		this.implOpMode1 = template.implOpMode1;
		this.implOpMode2 = template.implOpMode2;
		this.keySet1 = template.keySet1;
		this.keySet2 = template.keySet2;

		int i = 0;
		
		if(pred1 != null) {
			for(PactConnection c : conn1) {
				PactConnection cc = new PactConnection(c, pred1.get(i++), this); 
				this.input1.add(cc);
			}
		}
		
		if(pred2 != null) {
			i = 0;
			for(PactConnection c : conn2) {
				PactConnection cc = new PactConnection(c, pred2.get(i++), this); 
				this.input2.add(cc);
			}
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
		ArrayList<List<PactConnection>> inputs = new ArrayList<List<PactConnection>>(2);
		inputs.add(0, input1);
		inputs.add(1, input2);
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
		
		
		// step down to all producer nodes for second input and calculate alternative plans
		final int inputSize2 = this.input2.size();
		@SuppressWarnings("unchecked")
		List<? extends OptimizerNode>[] inPlans2 = new List[inputSize2];
		for(int i = 0; i < inputSize2; ++i) {
			inPlans2[i] = this.input2.get(i).getSourcePact().getAlternativePlans(estimator);
		}

		// build all possible alternative plans for second input of this node
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
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readReadsAnnotation() {
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get readSet annotation from stub
		ReadsFirst readSet1Annotation = c.getUserCodeClass().getAnnotation(ReadsFirst.class);
		ReadsSecond readSet2Annotation = c.getUserCodeClass().getAnnotation(ReadsSecond.class);
		
		// extract readSets from annotations
		if(readSet1Annotation == null) {
			this.reads1 = null;
		} else {
			this.reads1 = readSet1Annotation.fields();
			Arrays.sort(this.reads1);
		}
		
		if(readSet2Annotation == null) {
			this.reads2 = null;
		} else {
			this.reads2 = readSet2Annotation.fields();
			Arrays.sort(this.reads2);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readCopyProjectionAnnotations()
	 */
	@Override
	protected void readCopyProjectionAnnotations() {
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get updateSet annotation from stub
		ImplicitOperationFirst implOp1Annotation = c.getUserCodeClass().getAnnotation(ImplicitOperationFirst.class);
		ImplicitOperationSecond implOp2Annotation = c.getUserCodeClass().getAnnotation(ImplicitOperationSecond.class);
		
		// set sets to null by default
		this.implOpMode1 = null;
		this.explCopies1 = null;
		this.explProjections1 = null;
		
		if(implOp1Annotation != null) {
			switch(implOp1Annotation.implicitOperation()) {
			case Copy:
				// implicit copy -> we have explicit projection
				ExplicitProjectionsFirst explProjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitProjectionsFirst.class);
				if(explProjAnnotation != null) {
					this.implOpMode1 = ImplicitOperationMode.Copy;
					this.explProjections1 = explProjAnnotation.fields();
					Arrays.sort(this.explProjections1);
				}
				break;
			case Projection:
				// implicit projection -> we have explicit copies
				ExplicitCopiesFirst explCopyjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitCopiesFirst.class);
				if(explCopyjAnnotation != null) {
					this.implOpMode1 = ImplicitOperationMode.Projection;
					this.explCopies1 = explCopyjAnnotation.fields();
					Arrays.sort(this.explCopies1);
				}
				break;
			}
		}
		
		// set sets to null by default
		this.implOpMode2 = null;
		this.explCopies2 = null;
		this.explProjections2 = null;

		if(implOp2Annotation != null) {
			switch(implOp2Annotation.implicitOperation()) {
			case Copy:
				// implicit copy -> we have explicit projection
				ExplicitProjectionsSecond explProjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitProjectionsSecond.class);
				if(explProjAnnotation != null) {
					this.implOpMode2 = ImplicitOperationMode.Copy;
					this.explProjections2 = explProjAnnotation.fields();
					Arrays.sort(this.explProjections2);
				}
				break;
			case Projection:
				// implicit projection -> we have explicit copies
				ExplicitCopiesSecond explCopyjAnnotation = c.getUserCodeClass().getAnnotation(ExplicitCopiesSecond.class);
				if(explCopyjAnnotation != null) {
					this.implOpMode2 = ImplicitOperationMode.Projection;
					this.explCopies2 = explCopyjAnnotation.fields();
					Arrays.sort(this.explCopies2);
				}
				break;
			}
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputSchema(java.util.List)
	 */
	@Override
	public int[] computeOutputSchema(List<int[]> inputSchemas) {

		if(inputSchemas.size() != 2)
			throw new IllegalArgumentException("TwoInputNode requires exactly 2 input nodes");
		
		// fields that are kept constant from the inputs
		int[] constFields1 = null;
		int[] constFields2 = null;
		
		// explicit writes must be defined
		if(explWrites == null) {
			return null;
		}
		
		if(implOpMode1 == null) {
			constFields1 = null;
		} else {
			
			switch(implOpMode1) {
			case Copy:
				// implicit copy -> we keep everything, except for explicit projections
				if(this.explProjections1 != null) {
					constFields1 = FieldSetOperations.setDifference(inputSchemas.get(0), this.explProjections1);
				} else {
					constFields1 = null;
				}
				break;
			case Projection:
				// implicit projection -> we keep only explicit copies
				constFields1 = this.explCopies1;
				break;
			}
		}
		
		if(implOpMode2 == null) {
			constFields2 = null;
		} else {
			
			switch(implOpMode2) {
			case Copy:
				// implicit copy -> we keep everything, except for explicit projections
				if(this.explProjections2 != null) {
					constFields2 = FieldSetOperations.setDifference(inputSchemas.get(1), this.explProjections2);
				} else {
					constFields2 = null;
				}
				break;
			case Projection:
				// implicit projection -> we keep only explicit copies
				constFields2 = this.explCopies2;
				break;
			}
		}
		
		if(constFields1 != null && constFields2 != null) {
			// output schema are kept fields plus explicit writes
			return FieldSetOperations.unionSets( 
				FieldSetOperations.unionSets(constFields1, constFields2),
				this.explWrites);
		} else {
			return null;
		}
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#deriveOutputSchema()
	 */
	@Override
	public void deriveOutputSchema() {
		
		if(this.input1.size() > 1 || this.input2.size() > 1) {
			throw new UnsupportedOperationException("Can not compute output schema for nodes with unioned inputs");
		}
		
		// collect input schema of node
		List<int[]> inputSchemas = new ArrayList<int[]>(2);
		inputSchemas.add(this.input1.get(0).getSourcePact().getOutputSchema());
		inputSchemas.add(this.input2.get(0).getSourcePact().getOutputSchema());
		
		// compute output schema given the node's input schemas
		this.outputSchema = computeOutputSchema(inputSchemas);
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isValidInputSchema(int, int[])
	 */
	@Override
	public boolean isValidInputSchema(int input, int[] inputSchema) {
		
		if(input < 0 || input > 1)
			throw new IndexOutOfBoundsException("TwoInputNode has inputs 0 or 1");
		
		// check for first input
		if(input == 0) {
			// check that we can perform all required reads on the input schema
			if(this.reads1 != null && !FieldSetOperations.fullyContained(inputSchema, this.reads1))
				return false;
			// check that the input schema contains all keys
			if(this.keySet1 != null && !FieldSetOperations.fullyContained(inputSchema, this.keySet1))
				return false;
			// check that implicit mode is set
			if(this.implOpMode1 == null) {
				return false;
			}
			// check that explicit projections can be performed
			if(this.implOpMode1 == ImplicitOperationMode.Copy && 
					!FieldSetOperations.fullyContained(inputSchema, this.explProjections1))
				return false;
			// check that explicit copies can be performed
			if(this.implOpMode1 == ImplicitOperationMode.Projection &&
					!FieldSetOperations.fullyContained(inputSchema, this.explCopies1))
				return false;
		// check for second input
		} else {
			// check that we can perform all required reads on the input schema
			if(this.reads2 != null && !FieldSetOperations.fullyContained(inputSchema, this.reads2))
				return false;
			// check that the input schema contains all keys
			if(this.keySet2 != null && !FieldSetOperations.fullyContained(inputSchema, this.keySet2))
				return false;
			// check that implicit mode is set
			if(this.implOpMode2 == null) {
				return false;
			}
			// check that explicit projections can be performed
			if(this.implOpMode2 == ImplicitOperationMode.Copy && 
					!FieldSetOperations.fullyContained(inputSchema, this.explProjections2))
				return false;
			// check that explicit copies can be performed
			if(this.implOpMode2 == ImplicitOperationMode.Projection &&
					!FieldSetOperations.fullyContained(inputSchema, this.explCopies2))
				return false;
		}
		
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getReadSet(int)
	 */
	@Override
	public int[] getReadSet(int input) {

		switch(input) {
		case 0:
			return this.reads1;
		case 1:
			return this.reads2;
		case -1:
			return FieldSetOperations.unionSets(this.reads1, this.reads2);
		default:
			throw new IndexOutOfBoundsException();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int)
	 */
	@Override
	public int[] getWriteSet(int input) {
		
		if(this.input1.size() > 1 || this.input2.size() > 1) {
			throw new UnsupportedOperationException("Can not compute output schema for nodes with unioned inputs");
		}
		
		// get the input schemas of the node
		List<int[]> inputSchemas = new ArrayList<int[]>(2);
		inputSchemas.add(this.input1.get(0).getSourcePact().getOutputSchema());
		inputSchemas.add(this.input2.get(0).getSourcePact().getOutputSchema());
		
		// compute and return the write set for the node's input schemas
		return this.getWriteSet(input, inputSchemas);
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getWriteSet(int, java.util.List)
	 */
	@Override
	public int[] getWriteSet(int input, List<int[]> inputSchemas) {

		if(inputSchemas.size() != 2)
			throw new IllegalArgumentException("TwoInputNode requires exactly 2 input nodes");
		
		switch(input) {
		// compute write set for first input
		case 0:
			if(implOpMode1 != null) {
				switch(implOpMode1) {
				case Copy:
					// implicit copy -> write set are all explicit projections plus writes
					if(this.explProjections1 != null) {
						return FieldSetOperations.unionSets(this.explProjections1, this.explWrites);
					} else {
						return null;
					}
				case Projection:
					// implicit projection -> write set are all input fields minus copied fields plus writes
					if(this.explCopies1 != null) {
						return FieldSetOperations.unionSets(
							FieldSetOperations.setDifference(inputSchemas.get(0), this.explCopies1),
							this.explWrites);
					} else {
						return null;
					}
				default:
					return null;
				}
			} else {
				return null;
			}
		// compute write set for second input
		case 1:
			if(implOpMode2 != null) {
				switch(implOpMode2) {
				case Copy:
					// implicit copy -> write set are all explicit projections plus writes
					if(this.explProjections2 != null) {
						return FieldSetOperations.unionSets(this.explProjections2, this.explWrites);
					} else {
						return null;
					}
				case Projection:
					// implicit projection -> write set are all input fields minus copied fields plus writes
					if(this.explCopies2 != null) {
						return FieldSetOperations.unionSets(
								FieldSetOperations.setDifference(inputSchemas.get(1), this.explCopies2),
								this.explWrites);
					} else {
						return null;
					}
				default:
					return null;
				}
			} else {
				return null;
			}
		// compute write set for both inputs
		case -1:
			if(this.implOpMode1 != null && this.implOpMode2 != null && this.explWrites != null) {
				
				// sets of projected (and hence written) fields
				int[] projection1 = null;
				int[] projection2 = null;
				
				switch(this.implOpMode1) {
				case Copy:
					// implicit copy -> explicit projection
					projection1 = this.explProjections1;
					break;
				case Projection:
					// implicit projection -> input schema minus copied fields
					if(this.explCopies1 != null) {
						projection1 = FieldSetOperations.setDifference(inputSchemas.get(0), this.explCopies1);
					} else {
						return null;
					}
					break;
				default:
					return null;
				}
				
				switch(this.implOpMode2) {
				case Copy:
					// implicit copy -> explicit projection
					projection2 = this.explProjections2;
					break;
				case Projection:
					// implicit projection -> input schema minus copied fields
					if(this.explCopies2 != null) {
						projection2 = FieldSetOperations.setDifference(inputSchemas.get(1), this.explCopies2);
					} else {
						return null;
					}
					break;
				default:
					return null;
				}
		
				if(projection1 != null && projection2 != null) {
					// write set are projected and explicitly written fields
					return FieldSetOperations.unionSets(
							FieldSetOperations.unionSets(projection1, projection2),
							this.explWrites);
					
				} else {
					return null;
				}
				
			} else {
				return null;
			}
		default:
			throw new IndexOutOfBoundsException();
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
	
		long outputSize = 0;
		long numRecords = 0;
		for(PactConnection c : this.input1) {
			OptimizerNode pred = c.getSourcePact();
			
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its output size or number of records, we a pessimistic and return "unknown" as well
				if(pred.estimatedOutputSize == -1 || pred.estimatedNumRecords == -1) {
					outputSize = -1;
					break;
				}
				
				outputSize += pred.estimatedOutputSize;
				numRecords += pred.estimatedNumRecords;
			}
		}

		double avgWidth = -1;

		if(outputSize != -1) {
			avgWidth = outputSize / (double)numRecords;
			if(avgWidth < 1)
				avgWidth = 1;
		}
		

		for(PactConnection c : this.input2) {
			OptimizerNode pred = c.getSourcePact();
			
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its output size or number of records, we a pessimistic and return "unknown" as well
				if(pred.estimatedOutputSize == -1) {
					return avgWidth;
				}
				
				outputSize += pred.estimatedOutputSize;
				numRecords += pred.estimatedNumRecords;
			}
		}
		
		if(outputSize != -1) {
			avgWidth += outputSize / (double)numRecords;
			if(avgWidth < 2)
				avgWidth = 2;
		}

		return avgWidth;
	}

	/**
	 * Returns the key fields of the given input.
	 * 
	 * @param input The input for which key fields must be returned.
	 * @return the key fields of the given input.
	 */
	public int[] getInputKeySet(int input) {
		switch(input) {
		case 0: return keySet1;
		case 1: return keySet2;
		default: throw new IndexOutOfBoundsException();
		}
	}
	
	public boolean isFieldKept(int input, int fieldNumber) {
		
		switch(input) {
		case 0:
			if (implOpMode1 == null) {
				return false;
			}
			switch (implOpMode1) {
			case Projection:
				return (explCopies1 == null ? false : 
					Arrays.binarySearch(explCopies1, fieldNumber) >= 0);
			case Copy:
				return (explProjections1 == null || explWrites == null ? false :  
					Arrays.binarySearch(FieldSetOperations.unionSets(explWrites, explProjections1), fieldNumber) < 0);
			default:
					return false;
			}
		case 1:
			if (implOpMode2 == null) {
				return false;
			}
			switch (implOpMode2) {
			case Projection:
				return (explCopies2 == null ? false : 
					Arrays.binarySearch(explCopies2, fieldNumber) >= 0);
			case Copy:
				return (explProjections2 == null || explWrites == null ? false :  
					Arrays.binarySearch(FieldSetOperations.unionSets(explWrites, explProjections2), fieldNumber) < 0);
			default:
					return false;
			}
		default:
			throw new IndexOutOfBoundsException();
		}
	}
	
}
