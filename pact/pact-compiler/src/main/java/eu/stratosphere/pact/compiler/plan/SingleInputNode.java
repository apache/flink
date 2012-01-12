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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.util.FieldSetOperations;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a single input.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class SingleInputNode extends OptimizerNode {
	
	private List<OptimizerNode> cachedPlans; // a cache for the computed alternative plans

	final protected List<PactConnection> input = new ArrayList<PactConnection>(); // The list of input edges
	
	// ------------- Stub Annotations
	
	protected int[] readSet; // set of fields that are read by the stub
	
	protected int[] updateSet; // set of fields that are modified by the stub
	
	protected int[] constantSet; // set of fields that remain constant from input to output 
	
	protected int[] keySet; // The set of key fields (order is relevant!)

	// ------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public SingleInputNode(SingleInputContract<?> pactContract) {
		super(pactContract);
		this.keySet = pactContract.getKeyColumnNumbers(0);
		readReadSetAnnotation();
		readConstantSetAnnotation();
	}

	/**
	 * Copy constructor to create a copy of a node with a different predecessor. The predecessor
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
	protected SingleInputNode(SingleInputNode template, List<OptimizerNode> pred, List<PactConnection> conn,
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
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) throws CompilerException {
		// get the predecessor node
		List<Contract> children = ((SingleInputContract<?>) getPactContract()).getInputs();
		
		for(Contract child : children) {
			OptimizerNode pred = contractToNode.get(child);
	
			// create a connection
			PactConnection conn = new PactConnection(pred, this);
			addInputConnection(conn);
			pred.addOutgoingConnection(conn);
	
			// see if an internal hint dictates the strategy to use
			Configuration conf = getPactContract().getParameters();
			String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
			if (shipStrategy != null) {
				if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
					conn.setShipStrategy(ShipStrategy.FORWARD);
				} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION.equals(shipStrategy)) {
					conn.setShipStrategy(ShipStrategy.PARTITION_HASH);
				} else {
					throw new CompilerException("Invalid hint for the shipping strategy of a single input connection: "
						+ shipStrategy);
				}
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
		
		for(int i = 0; i < alternativeSubPlanCominations.size(); ++i) {
			// check, whether the two children have the same
			// sub-plan in the common part before the branches
			if (!areBranchCompatible(alternativeSubPlanCominations.get(i), null)) {
				alternativeSubPlanCominations.remove(i);
				// as we removed plan #i we have to test at index #i again which
				// has an new plan now
				--i;
			}
		}

		List<OptimizerNode> outputPlans = new ArrayList<OptimizerNode>();

		computeValidPlanAlternatives(alternativeSubPlanCominations, estimator,  outputPlans);
		
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
	 * @param alternativeSubPlanCominations	 	List with all sub-plan-combinations
	 * @param estimator							Cost estimator to be used
	 * @param outputPlans						The generated output plans (is expected to be a list where new plans can be added)
	 */
	protected abstract void computeValidPlanAlternatives(List<List<OptimizerNode>> alternativeSubPlanCominations,
			CostEstimator estimator, List<OptimizerNode> outputPlans);
	
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
			for(PactConnection c : this.input) {
				OptimizerNode n = c.getSourcePact();
				if (n != null) {
					n.accept(visitor);
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

		// we have to look for closing branches for all input-pair-combinations in the union case

		final int sizeInput = this.input.size();
		
		// check all all unioned inputs pair combination
		// all unioned inputs from input1
		for(int i = 0; i < sizeInput; ++i) {
			PactConnection pc1 = this.input.get(i);
			for(int j = i+1; j < sizeInput; ++j) {
				PactConnection pc2 = this.input.get(j);

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
	
	protected void readReadSetAnnotation() {
		
		SingleInputContract<?> c = (SingleInputContract<?>)super.getPactContract();
		
		// get readSet annotation from stub
		ReadSet readSetAnnotation = c.getUserCodeClass().getAnnotation(ReadSet.class);
		
		// extract readSet from annotation
		if(readSetAnnotation == null) {
			this.readSet = null;
			return;
		} else {
			this.readSet = readSetAnnotation.fields();
		}

		if(c instanceof ReduceContract) {
			// merge read and key sets
			int[] keySet = c.getKeyColumnNumbers(0);
			Arrays.sort(keySet);
			Arrays.sort(this.readSet);
			this.readSet = FieldSetOperations.unionSets(keySet, this.readSet);
			
		} else {
			Arrays.sort(this.readSet);
		}
	}
	
	protected void readConstantSetAnnotation() {
		
		SingleInputContract<?> c = (SingleInputContract<?>)super.getPactContract();
		
		// get updateSet annotation from stub
		ConstantSet updateSetAnnotation = c.getUserCodeClass().getAnnotation(ConstantSet.class);
		
		// extract readSet from annotation
		if(updateSetAnnotation == null) {
			this.updateSet = null;
			this.constantSet = null;
			return;
		} else {
			
			switch(updateSetAnnotation.setMode()) {
			case Update:
				// we have a write set
				this.updateSet = updateSetAnnotation.fields();
				this.constantSet = null;
				Arrays.sort(this.updateSet);
				return;
			case Constant:
				// we have a constant set
				this.updateSet = null;
				this.constantSet = updateSetAnnotation.fields();
				Arrays.sort(this.constantSet);
				return;
			default:
				this.updateSet = null;
				this.constantSet = null;
				return;
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
		
		for(PactConnection pc : this.getInputConnections()) {
			if(pc.getSourcePact().outputSchema == null) {
				this.outputSchema = null;
				return;
			}
			outputSchema = FieldSetOperations.unionSets(outputSchema, pc.getSourcePact().outputSchema);
		}
	}
	
	public int[] getReadSet() {
		return this.readSet;
	}
	
	public int[] getUpdateSet() {
		return this.updateSet;
	}
	
	public int[] getConstantSet() {
		return this.constantSet;
	}
	
	public int[] getKeySet() {
		return this.keySet;
	}
}
